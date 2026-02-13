-- Step 6 Phase B: Load all staging CSVs, build 5 consumer tables with IDs, export to final CSVs (no header).
-- Placeholders: {threads}, {memLimit}, {tempDir}, {staging}

SET threads = {threads};
SET memory_limit = '{memLimit}';
SET temp_directory = '{tempDir}';
SET preserve_insertion_order = false;

CREATE TEMP TABLE catalog AS SELECT * FROM read_csv_auto('{staging}/catalog.csv', header=true);
CREATE TEMP TABLE catalog_enriched AS SELECT * FROM read_csv_auto('{staging}/catalog_enriched.csv', header=true);
CREATE TEMP TABLE offsets AS SELECT * FROM read_csv_auto('{staging}/offsets.csv', header=true);
CREATE TEMP TABLE enc_keys AS SELECT * FROM read_csv_auto('{staging}/enc_keys.csv', header=true);
CREATE TEMP TABLE frame_index AS SELECT * FROM read_csv_auto('{staging}/frame_index.csv', header=true);
CREATE TEMP TABLE existing_paths AS SELECT * FROM read_csv_auto('{staging}/existing_paths.csv', header=true);

-- 1) tag_names: distinct tags from catalog.tags_normalized, id = row_number
CREATE TEMP TABLE _tag_names AS
SELECT row_number() OVER (ORDER BY name) AS id, name
FROM (
    SELECT DISTINCT trim(lower(tag)) AS name
    FROM (
        SELECT unnest(regexp_split_to_array(CAST(COALESCE(c.tags_normalized,'') AS VARCHAR), '[|,]')) AS tag
        FROM catalog c
        WHERE CAST(c.tags_normalized AS VARCHAR) IS NOT NULL AND trim(CAST(c.tags_normalized AS VARCHAR)) != ''
    )
    WHERE trim(tag) != ''
);
COPY (SELECT id, name FROM _tag_names ORDER BY id) TO '{staging}/final_tag_names.csv' (HEADER false, DELIMITER ',');

-- 2) tags: (id, ruid, tag_id, created_at, updated_at). DuckDB does not allow COPY after WITH; materialize first.
CREATE TEMP TABLE _tags_result AS
WITH tag_pairs AS (
    SELECT trim(c.ruid) AS ruid, trim(lower(unnest(regexp_split_to_array(CAST(COALESCE(c.tags_normalized,'') AS VARCHAR), '[|,]')))) AS tag_name
    FROM catalog c
    WHERE CAST(c.tags_normalized AS VARCHAR) IS NOT NULL AND trim(CAST(c.tags_normalized AS VARCHAR)) != ''
),
tags_resolved AS (
    SELECT DISTINCT p.ruid, tn.id AS tag_id
    FROM tag_pairs p
    JOIN _tag_names tn ON tn.name = p.tag_name
    WHERE p.tag_name != ''
),
tags_numbered AS (
    SELECT row_number() OVER () AS id, ruid, tag_id,
        strftime(current_timestamp, '%Y-%m-%d %H:%M:%S') AS created_at,
        strftime(current_timestamp, '%Y-%m-%d %H:%M:%S') AS updated_at
    FROM tags_resolved
)
SELECT id, ruid, tag_id, created_at, updated_at FROM tags_numbered;
COPY (SELECT id, ruid, tag_id, created_at, updated_at FROM _tags_result) TO '{staging}/final_tags.csv' (HEADER false, DELIMITER ',');

-- 3) Enriched view: catalog_enriched + offsets, dedupe by ruid (first-wins). Materialize so we reuse for animation_frames and assets.
-- Cast offset_x/offset_y to DOUBLE (CSV may read as VARCHAR); COALESCE with 0.0 to avoid type mismatch.
CREATE TEMP TABLE enriched AS
SELECT ce.ruid, ce.category, ce.subcategory, ce.output_subdir, ce.name, ce.asset_type, ce.relative_path, ce.suffix,
    COALESCE(TRY_CAST(o.offset_x AS DOUBLE), 0.0) AS offset_x, COALESCE(TRY_CAST(o.offset_y AS DOUBLE), 0.0) AS offset_y
FROM (
    SELECT *, row_number() OVER (PARTITION BY lower(trim(ruid)) ORDER BY relative_path) AS rn
    FROM catalog_enriched
) ce
LEFT JOIN (SELECT ruid, output_subdir, offset_x, offset_y FROM offsets) o ON lower(trim(ce.ruid)) = lower(trim(o.ruid)) AND trim(ce.output_subdir) = trim(o.output_subdir)
WHERE ce.rn = 1;

-- 4) animation_frames: frame_index + enriched for image_path (output_subdir/frame_ruid.png), offset_x/y.
--    Materialize so we can reuse for clip has_data and thumbnail_ruid.
CREATE TEMP TABLE _anim_frames AS
SELECT
    row_number() OVER () AS id,
    fi.clip_ruid,
    fi.frame_index,
    fi.frame_ruid,
    fi.frame_duration_ms,
    COALESCE(e.output_subdir, 'Unknown') || '/' || fi.frame_ruid || '.png' AS image_path,
    COALESCE(TRY_CAST(e.offset_x AS DOUBLE), 0.0) AS offset_x,
    COALESCE(TRY_CAST(e.offset_y AS DOUBLE), 0.0) AS offset_y
FROM frame_index fi
LEFT JOIN enriched e ON lower(trim(e.ruid)) = lower(trim(fi.frame_ruid));

COPY (SELECT id, clip_ruid, frame_index, frame_ruid, frame_duration_ms, image_path, offset_x, offset_y FROM _anim_frames)
TO '{staging}/final_animation_frames.csv' (HEADER false, DELIMITER ',');

-- 4b) Clip metadata: for each clip_ruid, compute has_data (1 only if ALL frames exist) and thumbnail_ruid (median frame).
CREATE TEMP TABLE _clip_meta AS
WITH frame_exists AS (
    SELECT af.clip_ruid, af.frame_index, af.frame_ruid, af.image_path,
        CASE WHEN EXISTS (SELECT 1 FROM existing_paths p WHERE trim(p.relative_path) = af.image_path) THEN 1 ELSE 0 END AS frame_exists
    FROM _anim_frames af
),
clip_stats AS (
    SELECT clip_ruid,
        MIN(frame_exists) AS all_exist,                             -- 1 only if every frame exists
        COUNT(*) AS frame_count
    FROM frame_exists
    GROUP BY clip_ruid
),
-- Median frame: pick the frame at the middle index (floor of count/2)
median_frame AS (
    SELECT af.clip_ruid, af.frame_ruid AS thumbnail_ruid
    FROM _anim_frames af
    JOIN clip_stats cs ON af.clip_ruid = cs.clip_ruid
    WHERE af.frame_index = CAST(cs.frame_count / 2 AS INTEGER)
)
SELECT cs.clip_ruid, cs.all_exist AS has_data,
    mf.thumbnail_ruid
FROM clip_stats cs
LEFT JOIN median_frame mf ON cs.clip_ruid = mf.clip_ruid;

-- 5) assets: one per ruid from enriched.
--    Non-clips: has_data = 1 if output_subdir/ruid.png in existing_paths.
--    Clips: has_data from _clip_meta (all frames exist); thumbnail_ruid from _clip_meta (median frame).
--    DuckDB does not allow COPY after WITH; materialize first.
CREATE TEMP TABLE _assets_result AS
WITH asset_rows AS (
    SELECT e.ruid, e.name, e.asset_type, e.category, e.subcategory,
        e.output_subdir || '/' || e.ruid || '.png' AS path,
        e.output_subdir, e.ruid || '.png' AS filename,
        e.offset_x, e.offset_y,
        CASE
            WHEN lower(trim(e.asset_type)) = 'animationclip' THEN COALESCE(cm.has_data, 0)
            ELSE CASE WHEN EXISTS (SELECT 1 FROM existing_paths p WHERE trim(p.relative_path) = trim(e.output_subdir) || '/' || trim(e.ruid) || '.png') THEN 1 ELSE 0 END
        END AS has_data,
        CASE
            WHEN lower(trim(e.asset_type)) = 'animationclip' THEN cm.thumbnail_ruid
            ELSE CAST(NULL AS VARCHAR)
        END AS thumbnail_ruid
    FROM enriched e
    LEFT JOIN _clip_meta cm ON lower(trim(e.ruid)) = lower(trim(cm.clip_ruid))
),
assets_numbered AS (
    SELECT row_number() OVER () AS id, ruid, name, asset_type, category, subcategory, path, thumbnail_ruid, has_data, offset_x, offset_y, output_subdir, filename,
        strftime(current_timestamp, '%Y-%m-%d %H:%M:%S') AS created_at,
        strftime(current_timestamp, '%Y-%m-%d %H:%M:%S') AS updated_at
    FROM asset_rows
)
SELECT id, ruid, name, asset_type, category, subcategory, path, thumbnail_ruid, has_data, offset_x, offset_y, output_subdir, filename, created_at, updated_at FROM assets_numbered;
COPY (SELECT id, ruid, name, asset_type, category, subcategory, path, thumbnail_ruid, has_data, offset_x, offset_y, output_subdir, filename, created_at, updated_at FROM _assets_result)
TO '{staging}/final_assets.csv' (HEADER false, DELIMITER ',');

-- 6) cache_locations: ruid, cache_path (= relative_path), asset_type, suffix from enriched (all rows from catalog_enriched, not deduped)
COPY (
    SELECT trim(ruid) AS ruid, trim(relative_path) AS cache_path, trim(asset_type) AS asset_type, trim(suffix) AS suffix
    FROM catalog_enriched
    ORDER BY ruid, relative_path
) TO '{staging}/final_cache_locations.csv' (HEADER false, DELIMITER ',');
