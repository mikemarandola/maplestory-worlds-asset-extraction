PRAGMA journal_mode = OFF;
PRAGMA synchronous = OFF;
PRAGMA page_size = 4096;

CREATE TABLE tag_names (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE tags (id INTEGER PRIMARY KEY, ruid TEXT NOT NULL, tag_id INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(ruid, tag_id), FOREIGN KEY (tag_id) REFERENCES tag_names(id));
CREATE TABLE assets (id INTEGER PRIMARY KEY, ruid TEXT NOT NULL UNIQUE, name TEXT, asset_type TEXT, category TEXT, subcategory TEXT, path TEXT, thumbnail_ruid TEXT, has_data INTEGER DEFAULT 0, offset_x INTEGER DEFAULT 0, offset_y INTEGER DEFAULT 0, output_subdir TEXT, filename TEXT, created_at TEXT, updated_at TEXT);
CREATE TABLE animation_frames (id INTEGER PRIMARY KEY, clip_ruid TEXT, frame_index INTEGER, frame_ruid TEXT, frame_duration_ms INTEGER, image_path TEXT, offset_x INTEGER DEFAULT 0, offset_y INTEGER DEFAULT 0);
CREATE TABLE cache_locations (ruid TEXT NOT NULL, cache_path TEXT, asset_type TEXT, suffix TEXT);

.mode csv
.import '{final_tag_names}' tag_names
.import '{final_tags}' tags
.import '{final_assets}' assets
.import '{final_animation_frames}' animation_frames
.import '{final_cache_locations}' cache_locations

CREATE INDEX idx_tag_names_name ON tag_names(name);
CREATE INDEX idx_tags_ruid ON tags(ruid);
CREATE INDEX idx_tags_tag_id ON tags(tag_id);
CREATE INDEX idx_assets_ruid ON assets(ruid);
CREATE INDEX idx_assets_asset_type ON assets(asset_type);
CREATE INDEX idx_assets_category ON assets(category);
CREATE INDEX idx_assets_has_data ON assets(has_data);
CREATE INDEX idx_anim_clip ON animation_frames(clip_ruid);
CREATE INDEX idx_anim_frame_ruid ON animation_frames(frame_ruid);
CREATE INDEX idx_anim_image_path ON animation_frames(image_path);
CREATE INDEX idx_cache_locations_ruid ON cache_locations(ruid);

SELECT 'tag_names: ' || COUNT(*) FROM tag_names;
SELECT 'tags: ' || COUNT(*) FROM tags;
SELECT 'assets: ' || COUNT(*) FROM assets;
SELECT 'animation_frames: ' || COUNT(*) FROM animation_frames;
SELECT 'cache_locations: ' || COUNT(*) FROM cache_locations;
