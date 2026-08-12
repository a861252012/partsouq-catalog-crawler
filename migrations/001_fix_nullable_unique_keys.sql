-- PartSouq crawler — schema migration
-- 可重複執行（idempotent）：每個 ALTER 前先檢查欄位目前是否仍是 nullable，
-- 是才執行 MODIFY；欄位已是 NOT NULL 時直接跳過。
--
-- 用法：
--   mysql -h 127.0.0.1 -P 3308 -u root -p partsouq_crawler < migrations/001_fix_nullable_unique_keys.sql


-- groups_t.code：唯一鍵 uq_group(category_id, code) 的組成欄位。
-- NULL 時 MySQL 唯一鍵不視為相同 → 會插入重複列。改 NOT NULL DEFAULT ''。
SET @col_null := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'groups_t'
    AND COLUMN_NAME = 'code' AND IS_NULLABLE = 'YES'
);
SET @sql := IF(@col_null > 0,
  'ALTER TABLE groups_t MODIFY code VARCHAR(16) NOT NULL DEFAULT ''''',
  'SELECT ''groups_t.code already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- parts.range_str：唯一鍵 uq_part(group_id, part_number, range_str) 組成欄位。
SET @col_null := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'parts'
    AND COLUMN_NAME = 'range_str' AND IS_NULLABLE = 'YES'
);
SET @sql := IF(@col_null > 0,
  'ALTER TABLE parts MODIFY range_str VARCHAR(64) NOT NULL DEFAULT ''''',
  'SELECT ''parts.range_str already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- vehicles.name：唯一鍵 uq_vehicle(model_id, model_code, name(120)) 組成欄位。
SET @col_null := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'vehicles'
    AND COLUMN_NAME = 'name' AND IS_NULLABLE = 'YES'
);
SET @sql := IF(@col_null > 0,
  'ALTER TABLE vehicles MODIFY name VARCHAR(256) NOT NULL DEFAULT ''''',
  'SELECT ''vehicles.name already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- vehicles.model_code：唯一鍵 uq_vehicle(model_id, model_code, name(120)) 組成欄位。
SET @col_null := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'vehicles'
    AND COLUMN_NAME = 'model_code' AND IS_NULLABLE = 'YES'
);
SET @sql := IF(@col_null > 0,
  'ALTER TABLE vehicles MODIFY model_code VARCHAR(128) NOT NULL DEFAULT ''''',
  'SELECT ''vehicles.model_code already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- parts.updated_at 索引（心跳查詢 SELECT MAX(updated_at) FROM parts 用）。
-- 已存在（idx_part_updated）時略過。
SET @idx_exists := (
  SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'parts'
    AND INDEX_NAME = 'idx_part_updated'
);
SET @sql := IF(@idx_exists = 0,
  'ALTER TABLE parts ADD INDEX idx_part_updated (updated_at)',
  'SELECT ''idx_part_updated already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
