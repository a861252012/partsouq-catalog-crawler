-- PartSouq crawler — schema migration
-- 可重複執行（idempotent）：先檢查所有 NULL 正規化後是否會撞到既有
-- unique key；任何一組有碰撞都在 UPDATE / ALTER 前 fail closed。
--
-- 執行前必須停止 crawler，避免 preflight 與 backfill 之間有新寫入。
-- Metadata lock 與 InnoDB row lock 最多等待 30 秒；逾時後停止並重跑，
-- 不要在 crawler 仍運作時提高 timeout。
--
-- 用法：
--   mysql -h 127.0.0.1 -P 3308 -u root -p partsouq_crawler < migrations/001_fix_nullable_unique_keys.sql

SET SESSION lock_wait_timeout = 30;
SET SESSION innodb_lock_wait_timeout = 30;

-- 欄位不存在不是「已經完成」。先確認 legacy schema 符合這支 migration
-- 的輸入契約，避免 information_schema 的 0 被誤當成 NOT NULL。
SET @required_column_count := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE() AND (
    (TABLE_NAME = 'groups_t' AND COLUMN_NAME = 'code') OR
    (TABLE_NAME = 'parts' AND COLUMN_NAME = 'range_str') OR
    (TABLE_NAME = 'vehicles' AND COLUMN_NAME = 'name') OR
    (TABLE_NAME = 'vehicles' AND COLUMN_NAME = 'model_code')
  )
);
DROP PROCEDURE IF EXISTS assert_partsouq_001_schema;
DELIMITER //
CREATE PROCEDURE assert_partsouq_001_schema()
BEGIN
  IF @required_column_count <> 4 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'migration 001: required legacy columns are missing';
  END IF;
END//
DELIMITER ;
CALL assert_partsouq_001_schema();
DROP PROCEDURE assert_partsouq_001_schema;

-- 先保存 nullable 狀態。已完成 migration 的欄位不再做 collision preflight、
-- backfill 或 ALTER，確保重跑不會誤判新版 schema 可合法存在的資料。
SET @groups_code_nullable := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'groups_t'
    AND COLUMN_NAME = 'code' AND IS_NULLABLE = 'YES'
);
SET @parts_range_nullable := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'parts'
    AND COLUMN_NAME = 'range_str' AND IS_NULLABLE = 'YES'
);
SET @vehicles_name_nullable := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'vehicles'
    AND COLUMN_NAME = 'name' AND IS_NULLABLE = 'YES'
);
SET @vehicles_model_code_nullable := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'vehicles'
    AND COLUMN_NAME = 'model_code' AND IS_NULLABLE = 'YES'
);

-- 001 執行時的 unique key 形狀：
--   uq_group(category_id, code)
--   uq_part(group_id, part_number, range_str)
--   uq_vehicle(model_id, model_code, name(120))
-- 對每個 key 模擬 NULL -> ''，並沿用欄位 collation / name prefix 語意。
SET @groups_code_collision := IF(@groups_code_nullable > 0, (
  SELECT COUNT(*) FROM (
    SELECT category_id, COALESCE(code, '') AS normalized_code
    FROM groups_t
    GROUP BY category_id, normalized_code
    HAVING COUNT(*) > 1
      AND SUM(CASE WHEN code IS NULL THEN 1 ELSE 0 END) > 0
  ) AS duplicate_group_identity
), 0);
SET @parts_range_collision := IF(@parts_range_nullable > 0, (
  SELECT COUNT(*) FROM (
    SELECT group_id, part_number, COALESCE(range_str, '') AS normalized_range
    FROM parts
    GROUP BY group_id, part_number, normalized_range
    HAVING COUNT(*) > 1
      AND SUM(CASE WHEN range_str IS NULL THEN 1 ELSE 0 END) > 0
  ) AS duplicate_part_identity
), 0);
SET @vehicles_identity_collision := IF(
  @vehicles_name_nullable > 0 OR @vehicles_model_code_nullable > 0,
  (
    SELECT COUNT(*) FROM (
      SELECT
        model_id,
        COALESCE(model_code, '') AS normalized_model_code,
        LEFT(COALESCE(name, ''), 120) AS normalized_name_prefix
      FROM vehicles
      GROUP BY model_id, normalized_model_code, normalized_name_prefix
      HAVING COUNT(*) > 1
        AND SUM(
          CASE WHEN model_code IS NULL OR name IS NULL THEN 1 ELSE 0 END
        ) > 0
    ) AS duplicate_vehicle_identity
  ),
  0
);

DROP PROCEDURE IF EXISTS assert_partsouq_001_normalization;
DELIMITER //
CREATE PROCEDURE assert_partsouq_001_normalization()
BEGIN
  IF @groups_code_collision > 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'migration 001: groups_t NULL-to-empty unique collision';
  END IF;
  IF @parts_range_collision > 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'migration 001: parts NULL-to-empty unique collision';
  END IF;
  IF @vehicles_identity_collision > 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'migration 001: vehicles NULL-to-empty unique collision';
  END IF;
END//
DELIMITER ;
CALL assert_partsouq_001_normalization();
DROP PROCEDURE assert_partsouq_001_normalization;

-- 所有 collision preflight 都通過後才 backfill。每個 UPDATE 都受原始
-- nullable 狀態保護，因此重跑 migration 是 no-op。
SET @sql := IF(@groups_code_nullable > 0,
  'UPDATE groups_t SET code = '''' WHERE code IS NULL',
  'SELECT ''groups_t.code already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := IF(@parts_range_nullable > 0,
  'UPDATE parts SET range_str = '''' WHERE range_str IS NULL',
  'SELECT ''parts.range_str already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := IF(@vehicles_name_nullable > 0,
  'UPDATE vehicles SET name = '''' WHERE name IS NULL',
  'SELECT ''vehicles.name already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := IF(@vehicles_model_code_nullable > 0,
  'UPDATE vehicles SET model_code = '''' WHERE model_code IS NULL',
  'SELECT ''vehicles.model_code already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- backfill 全部完成後才收緊欄位。
SET @sql := IF(@groups_code_nullable > 0,
  'ALTER TABLE groups_t MODIFY code VARCHAR(16) NOT NULL DEFAULT ''''',
  'SELECT ''groups_t.code already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := IF(@parts_range_nullable > 0,
  'ALTER TABLE parts MODIFY range_str VARCHAR(64) NOT NULL DEFAULT ''''',
  'SELECT ''parts.range_str already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := IF(@vehicles_name_nullable > 0,
  'ALTER TABLE vehicles MODIFY name VARCHAR(256) NOT NULL DEFAULT ''''',
  'SELECT ''vehicles.name already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := IF(@vehicles_model_code_nullable > 0,
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
