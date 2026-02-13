-- Step 5: Export clip list (animationclip rows with enc_key) and enc_key->ruid map for sprite lookup.
-- catalog_enriched has asset_type; enc_keys.csv has ruid, enc_key. Join to get clips with enc_key.
-- Placeholders: {threads}, {memLimit}, {tempDir}, {catalogEnrichedCsv}, {encKeysCsv}, {clipListCsv}, {encRuidMapCsv}

SET threads = {threads};
SET memory_limit = '{memLimit}';
SET temp_directory = '{tempDir}';
SET preserve_insertion_order = false;

CREATE TEMP TABLE catalog_enriched AS SELECT * FROM read_csv_auto('{catalogEnrichedCsv}', header=true);
CREATE TEMP TABLE enc_keys AS SELECT * FROM read_csv_auto('{encKeysCsv}', header=true);

-- Clips: asset_type = animationclip, join enc_keys on ruid to get enc_key. DuckDB does not allow COPY after WITH; materialize first.
CREATE TEMP TABLE _clip_list AS
SELECT ce.ruid, ce.relative_path, ce.suffix, ek.enc_key
FROM catalog_enriched ce
LEFT JOIN enc_keys ek ON lower(trim(ce.ruid)) = lower(trim(ek.ruid))
WHERE lower(trim(ce.asset_type)) = 'animationclip'
ORDER BY ce.ruid, ce.relative_path;

COPY (SELECT ruid, relative_path, suffix, enc_key FROM _clip_list)
TO '{clipListCsv}' (HEADER true, DELIMITER ',');

-- enc_ruid_map: enc_key, ruid (from enc_keys; for frame RUID lookup)
COPY (SELECT enc_key, ruid FROM enc_keys ORDER BY enc_key, ruid)
TO '{encRuidMapCsv}' (HEADER true, DELIMITER ',');
