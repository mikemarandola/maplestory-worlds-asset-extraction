-- Step 3: Export extract list from catalog_enriched (sprite, damageskin, avataritem, atlas, audioclip).
-- Dedupe images by (ruid, output_subdir) with suffix preference: dxt, win, png. Order for stable streaming.
-- Placeholders: {threads}, {memLimit}, {tempDir}, {catalogEnrichedCsv}, {outputCsv}

SET threads = {threads};
SET memory_limit = '{memLimit}';
SET temp_directory = '{tempDir}';
SET preserve_insertion_order = false;

CREATE TEMP TABLE catalog_enriched AS SELECT * FROM read_csv_auto('{catalogEnrichedCsv}', header=true);

-- DuckDB does not allow COPY after a WITH clause; materialize to temp table then COPY.
CREATE TEMP TABLE _extract_list AS
WITH image_types AS (
    SELECT *,
        CASE lower(trim(suffix)) WHEN 'dxt' THEN 1 WHEN 'win' THEN 2 WHEN 'png' THEN 3 ELSE 4 END AS suffix_pri
    FROM catalog_enriched
    WHERE lower(trim(asset_type)) IN ('sprite', 'damageskin', 'avataritem', 'atlas')
),
ranked_images AS (
    SELECT ruid, output_subdir, relative_path, suffix, asset_type,
        row_number() OVER (PARTITION BY lower(trim(ruid)), trim(coalesce(output_subdir, 'Unknown')) ORDER BY suffix_pri, relative_path) AS rn
    FROM image_types
),
audio_rows AS (
    SELECT ruid, output_subdir, relative_path, suffix, asset_type
    FROM catalog_enriched
    WHERE lower(trim(asset_type)) = 'audioclip'
),
deduped_images AS (
    SELECT ruid, output_subdir, relative_path, suffix, asset_type FROM ranked_images WHERE rn = 1
)
SELECT * FROM (
    (SELECT * FROM deduped_images) UNION ALL (SELECT * FROM audio_rows)
) AS u
ORDER BY asset_type, ruid, output_subdir, relative_path;

COPY (
    SELECT ruid, output_subdir, relative_path, suffix, asset_type
    FROM _extract_list
) TO '{outputCsv}' (HEADER true, DELIMITER ',');
