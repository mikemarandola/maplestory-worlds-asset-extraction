-- Step 4: Export sprite list (ruid, relative_path, suffix) for enc key extraction.
-- Filter: asset_type = sprite, suffix IN (win, dxt).
-- Placeholders: {threads}, {memLimit}, {tempDir}, {catalogEnrichedCsv}, {outputCsv}

SET threads = {threads};
SET memory_limit = '{memLimit}';
SET temp_directory = '{tempDir}';
SET preserve_insertion_order = false;

CREATE TEMP TABLE catalog_enriched AS SELECT * FROM read_csv_auto('{catalogEnrichedCsv}', header=true);

COPY (
    SELECT ruid, relative_path, suffix
    FROM catalog_enriched
    WHERE lower(trim(asset_type)) = 'sprite' AND lower(trim(suffix)) IN ('win', 'dxt')
    ORDER BY ruid, relative_path
) TO '{outputCsv}' (HEADER true, DELIMITER ',');
