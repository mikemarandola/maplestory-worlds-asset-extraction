-- Step 1: Build catalog from resources CSV. Expects columns: RUID, Category, Subcategory, Format, Tags (optional: Date, Name).
-- Placeholders {threads}, {memLimit}, {tempDir}, {inputCsv}, {outputCsv} are replaced by the PowerShell orchestrator.

SET threads = {threads};
SET memory_limit = '{memLimit}';
SET temp_directory = '{tempDir}';
SET preserve_insertion_order = false;

-- Read CSV with header. Required columns: RUID, Category, Subcategory, Format, Tags (exact names from discovery export).
CREATE TEMP TABLE raw AS
SELECT * FROM read_csv_auto('{inputCsv}', header=true, ignore_errors=false){limitClause};

-- DuckDB does not allow COPY after a WITH clause; materialize to temp table then COPY.
CREATE TEMP TABLE _catalog_result AS
WITH normalized AS (
    SELECT
        TRIM(CAST(raw."RUID" AS VARCHAR)) AS ruid,
        TRIM(COALESCE(CAST(raw."Category" AS VARCHAR), '')) AS category_raw,
        TRIM(COALESCE(CAST(raw."Subcategory" AS VARCHAR), '')) AS subcategory_raw,
        TRIM(COALESCE(CAST(raw."Format" AS VARCHAR), CAST(raw."Date" AS VARCHAR), '')) AS type_raw,
        TRIM(COALESCE(CAST(raw."Tags" AS VARCHAR), '')) AS tags_raw,
        row_number() OVER () AS rowid
    FROM raw
    WHERE LENGTH(TRIM(CAST(raw."RUID" AS VARCHAR))) = 32
      AND regexp_matches(TRIM(CAST(raw."RUID" AS VARCHAR)), '^[0-9a-fA-F]{32}$')
),
deduped AS (
    SELECT *, row_number() OVER (PARTITION BY lower(ruid) ORDER BY rowid) AS rn
    FROM normalized
),
cat_sanitized AS (
    SELECT ruid, rn,
        CASE WHEN TRIM(category_raw) = '' THEN 'Unknown'
             ELSE replace(replace(replace(replace(replace(replace(replace(replace(replace(TRIM(category_raw), '\\', '_'), '/', '_'), ':', '_'), '*', '_'), '?', '_'), '"', '_'), '<', '_'), '>', '_'), '|', '_') END AS cat,
        CASE WHEN TRIM(subcategory_raw) = '' THEN 'Unknown'
             ELSE replace(replace(replace(replace(replace(replace(replace(replace(replace(TRIM(subcategory_raw), '\\', '_'), '/', '_'), ':', '_'), '*', '_'), '?', '_'), '"', '_'), '<', '_'), '>', '_'), '|', '_') END AS sub,
        COALESCE(NULLIF(TRIM(type_raw), ''), ruid) AS type_val,
        TRIM(tags_raw) AS tags,
        replace(replace(TRIM(COALESCE(tags_raw, '')), ',', '|'), '  ', ' ') AS tags_normalized
    FROM deduped
    WHERE rn = 1
),
sanitized AS (
    SELECT ruid,
        CASE WHEN cat = '' THEN 'Unknown' ELSE cat END AS category,
        sub AS subcategory,
        CASE WHEN sub = '' OR sub = 'Unknown' THEN (CASE WHEN cat = '' THEN 'Unknown' ELSE cat END) ELSE (CASE WHEN cat = '' THEN 'Unknown' ELSE cat END) || '/' || sub END AS output_subdir,
        type_val AS asset_type,
        tags,
        tags_normalized
    FROM cat_sanitized
)
SELECT ruid, category, subcategory, output_subdir, asset_type, tags, tags_normalized FROM sanitized;

COPY (
    SELECT ruid, category, subcategory, output_subdir, asset_type, tags, tags_normalized
    FROM _catalog_result
    ORDER BY ruid
) TO '{outputCsv}' (HEADER true, DELIMITER ',');
