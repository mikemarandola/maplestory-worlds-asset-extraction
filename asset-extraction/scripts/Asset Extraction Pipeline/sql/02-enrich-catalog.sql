-- Step 2 Phase B: Join catalog.csv with cache_index.csv (LEFT JOIN on ruid). Output: catalog_enriched.csv
-- Placeholders: {threads}, {memLimit}, {tempDir}, {catalogCsv}, {cacheIndexCsv}, {outputCsv}

SET threads = {threads};
SET memory_limit = '{memLimit}';
SET temp_directory = '{tempDir}';
SET preserve_insertion_order = false;

CREATE TEMP TABLE catalog AS SELECT * FROM read_csv_auto('{catalogCsv}', header=true);
CREATE TEMP TABLE cache_index AS SELECT * FROM read_csv_auto('{cacheIndexCsv}', header=true);

-- Enrich: each catalog row joined with all matching cache rows (same ruid, case-insensitive)
COPY (
    SELECT
        c.category,
        c.subcategory,
        c.output_subdir,
        c.ruid,
        c.asset_type AS name,
        ci.relative_path,
        ci.suffix,
        ci.asset_type,
        ci.kind
    FROM catalog c
    LEFT JOIN cache_index ci ON lower(trim(c.ruid)) = lower(trim(ci.ruid))
    ORDER BY c.ruid, ci.relative_path
) TO '{outputCsv}' (HEADER true, DELIMITER ',');
