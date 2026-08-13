-- PartSouq crawler — schema migration 004
-- 1. vehicles 使用完整、binary SHA256 identity，取代 prefix/collation unique。
-- 2. v_parts 改讀 transactionally rebuilt published_parts snapshot。
-- 3. parts.seen_run_id 明確記錄 snapshot membership，取代秒級時間窗。
--
-- 執行前必須停止 crawler；ALTER / identity backfill 不是 online migration。
-- 可重複執行；不含 USE，請由 mysql 命令列指定資料庫。

SET SESSION lock_wait_timeout = 30;
SET SESSION innodb_lock_wait_timeout = 30;

-- vehicles.identity_hash
SET @vehicle_v5_exists := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle_identity_v5');
SET @vehicle_v4_exists := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle_identity');
SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND COLUMN_NAME='identity_hash');
SET @sql := IF(@col = 0,
  'ALTER TABLE vehicles ADD COLUMN identity_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL AFTER model_id',
  'SELECT ''vehicles.identity_hash already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Parser 支援的穩定規格欄位也必須持久化並納入 identity。
SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE()
  AND TABLE_NAME='vehicles' AND COLUMN_NAME='grade');
SET @sql := IF(@col = 0, 'ALTER TABLE vehicles ADD COLUMN grade VARCHAR(256) NULL AFTER prod_period',
  'SELECT ''vehicles.grade already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE()
  AND TABLE_NAME='vehicles' AND COLUMN_NAME='market');
SET @sql := IF(@col = 0, 'ALTER TABLE vehicles ADD COLUMN market VARCHAR(128) NULL AFTER grade',
  'SELECT ''vehicles.market already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE()
  AND TABLE_NAME='vehicles' AND COLUMN_NAME='engine');
SET @sql := IF(@col = 0, 'ALTER TABLE vehicles ADD COLUMN engine VARCHAR(256) NULL AFTER market',
  'SELECT ''vehicles.engine already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE()
  AND TABLE_NAME='vehicles' AND COLUMN_NAME='transmission');
SET @sql := IF(@col = 0,
  'ALTER TABLE vehicles ADD COLUMN transmission VARCHAR(256) NULL AFTER engine',
  'SELECT ''vehicles.transmission already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 與 src.repositories.vehicle_identity_hash 相同。ssd/vid/url 是請求 token
-- 或參數，不可放進 logical identity；token 輪替應更新同一列。
-- uq_vehicle_identity 若已存在，代表舊版 004 已執行；不在
-- 既有 unique 下重算公式，避免 token-only duplicate 收旂時直接
-- duplicate-key。公式升級由 005 的 staging column/index 處理。
UPDATE vehicles SET identity_hash = SHA2(CONCAT(
  OCTET_LENGTH(CAST(model_id AS CHAR)), ':', CAST(model_id AS CHAR),
  OCTET_LENGTH(COALESCE(model_code, '')), ':', COALESCE(model_code, ''),
  OCTET_LENGTH(COALESCE(name, '')), ':', COALESCE(name, ''),
  OCTET_LENGTH(COALESCE(description, '')), ':', COALESCE(description, ''),
  OCTET_LENGTH(COALESCE(options, '')), ':', COALESCE(options, ''),
  OCTET_LENGTH(COALESCE(prod_period, '')), ':', COALESCE(prod_period, ''),
  OCTET_LENGTH(COALESCE(grade, '')), ':', COALESCE(grade, ''),
  OCTET_LENGTH(COALESCE(market, '')), ':', COALESCE(market, ''),
  OCTET_LENGTH(COALESCE(engine, '')), ':', COALESCE(engine, ''),
  OCTET_LENGTH(COALESCE(transmission, '')), ':', COALESCE(transmission, '')
), 256)
WHERE @vehicle_v5_exists = 0 AND @vehicle_v4_exists = 0
  AND (identity_hash IS NULL OR identity_hash <> SHA2(CONCAT(
  OCTET_LENGTH(CAST(model_id AS CHAR)), ':', CAST(model_id AS CHAR),
  OCTET_LENGTH(COALESCE(model_code, '')), ':', COALESCE(model_code, ''),
  OCTET_LENGTH(COALESCE(name, '')), ':', COALESCE(name, ''),
  OCTET_LENGTH(COALESCE(description, '')), ':', COALESCE(description, ''),
  OCTET_LENGTH(COALESCE(options, '')), ':', COALESCE(options, ''),
  OCTET_LENGTH(COALESCE(prod_period, '')), ':', COALESCE(prod_period, ''),
  OCTET_LENGTH(COALESCE(grade, '')), ':', COALESCE(grade, ''),
  OCTET_LENGTH(COALESCE(market, '')), ':', COALESCE(market, ''),
  OCTET_LENGTH(COALESCE(engine, '')), ':', COALESCE(engine, ''),
  OCTET_LENGTH(COALESCE(transmission, '')), ':', COALESCE(transmission, '')
), 256));

SET @nullable := (SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND COLUMN_NAME='identity_hash' AND IS_NULLABLE='YES');
SET @sql := IF(@nullable > 0,
  'ALTER TABLE vehicles MODIFY identity_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL',
  'SELECT ''vehicles.identity_hash already NOT NULL''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 先加新 unique；若資料真的碰撞，ALTER 直接失敗並保留舊 unique，禁止
-- 靜默合併兩台車。新索引成功後才移除 prefix-based 舊索引。
SET @idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle_identity');
SET @sql := IF(@vehicle_v5_exists = 0 AND @vehicle_v4_exists = 0 AND @idx = 0,
  'ALTER TABLE vehicles ADD UNIQUE KEY uq_vehicle_identity (model_id, identity_hash)',
  'SELECT ''uq_vehicle_identity already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @old_idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle');
SET @sql := IF(@old_idx > 0,
  'ALTER TABLE vehicles DROP INDEX uq_vehicle',
  'SELECT ''legacy uq_vehicle already absent''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 只在 seen_run_id 首次導入時，清除 migration 前已開始且尚未
-- success 的 resume/receipt（保留 row_count 基準）。重跑本 migration
-- 不得再次清除新版 crawler 進度。
SET @seen_col_missing := (SELECT COUNT(*) = 0 FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='parts'
    AND COLUMN_NAME='seen_run_id');
DELETE cs FROM crawl_state cs
JOIN crawl_runs cr ON cr.run_key = cs.run_key
WHERE @seen_col_missing = 1 AND (cr.status IS NULL OR cr.status <> 'success');
UPDATE groups_t g
JOIN crawl_runs cr ON cr.run_key = g.fetched_run_key
SET g.fetched_run_key = NULL, g.fetched_status = NULL
WHERE @seen_col_missing = 1 AND (cr.status IS NULL OR cr.status <> 'success');

-- parts.seen_run_id：成功 publish 僅選本 logical run 明確觸及的 rows。
SET @sql := IF(@seen_col_missing = 1,
  'ALTER TABLE parts ADD COLUMN seen_run_id BIGINT NULL AFTER url',
  'SELECT ''parts.seen_run_id already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='parts'
    AND INDEX_NAME='idx_part_seen_run');
SET @sql := IF(@idx = 0,
  'ALTER TABLE parts ADD INDEX idx_part_seen_run (seen_run_id)',
  'SELECT ''idx_part_seen_run already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS published_parts (
  part_id        INT NOT NULL PRIMARY KEY,
  brand          VARCHAR(64) NOT NULL,
  model          VARCHAR(128) NOT NULL,
  vehicle_name   VARCHAR(256) NOT NULL,
  vehicle_code   VARCHAR(128) NOT NULL,
  prod_period    VARCHAR(64) NULL,
  part_name      VARCHAR(512) NULL,
  part_number    VARCHAR(64) NOT NULL,
  category_main  VARCHAR(256) NOT NULL,
  category_group VARCHAR(256) NULL,
  group_code     VARCHAR(16) NOT NULL,
  part_range     VARCHAR(64) NOT NULL,
  note           TEXT NULL,
  quantity       VARCHAR(16) NULL,
  code           VARCHAR(64) NULL,
  snapshot_at    DATETIME NOT NULL,
  KEY idx_published_part_number (part_number),
  KEY idx_published_brand_model (brand, model)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 首次 migration 必須複製「切換前的 v_parts」本身。舊 normalized rows
-- 的 updated_at 會被 failed/partial attempt 改寫，不能重建歷史 success。
SET @snapshot_empty := (SELECT COUNT(*) = 0 FROM published_parts);
SET @legacy_view_exists := (SELECT COUNT(*) FROM information_schema.VIEWS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='v_parts'
    AND LOWER(VIEW_DEFINITION) NOT LIKE '%published_parts%');
SET @sql := IF(@snapshot_empty = 1 AND @legacy_view_exists = 1,
  'INSERT INTO published_parts (
     part_id, brand, model, vehicle_name, vehicle_code, prod_period,
     part_name, part_number, category_main, category_group, group_code,
     part_range, note, quantity, code, snapshot_at
   )
   SELECT ROW_NUMBER() OVER (), brand, model, vehicle_name, vehicle_code, prod_period,
          part_name, part_number, category_main, category_group, group_code,
          part_range, note, quantity, code, NOW()
   FROM v_parts',
  'SELECT ''published snapshot already initialized or legacy v_parts absent''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- normalized parts 有資料卻無法讀取 legacy v_parts 時，不得把
-- current view 切成空表。這常見於舊 view 缺失或 migration 帳號
-- 沒有 VIEW_DEFINITION 權限；先 fail closed，由操作者補齊來源。
SET @normalized_parts_exist := (SELECT COUNT(*) > 0 FROM parts);
SET @snapshot_still_empty := (SELECT COUNT(*) = 0 FROM published_parts);
DROP PROCEDURE IF EXISTS assert_partsouq_004_snapshot_source;
DELIMITER //
CREATE PROCEDURE assert_partsouq_004_snapshot_source()
BEGIN
  IF @normalized_parts_exist = 1 AND @snapshot_still_empty = 1 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'migration 004: legacy v_parts unavailable or empty; refusing empty snapshot';
  END IF;
END//
DELIMITER ;
CALL assert_partsouq_004_snapshot_source();
DROP PROCEDURE assert_partsouq_004_snapshot_source;

CREATE OR REPLACE VIEW v_parts AS
SELECT brand, model, vehicle_name, vehicle_code, prod_period,
       part_name, part_number, category_main, category_group, group_code,
       part_range, note, quantity, code
FROM published_parts;
