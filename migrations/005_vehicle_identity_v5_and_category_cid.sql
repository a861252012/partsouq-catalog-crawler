-- PartSouq crawler — schema migration 005
-- 1. vehicle identity v5 納入 Body Style，並繼續排除 ssd/vid/url token。
-- 2. category 有 cid 時以 (vehicle_id, cid) 為穩定身分。
--
-- 執行前必須停止 crawler。舊 vehicle 規格無法安全還原，必須
-- 備份後顯式授權重建；其他 category cid 碰撞則 fail closed。
-- 可重複執行；uq_vehicle_identity_v5 存在就不重做 identity swap。

-- 舊 schema 沒有儲存 Grade/Market/Engine/Transmission/Body Style。對已有
-- normalized vehicle 原地補 NULL 無法還原真實 identity；下次爬取會插入
-- 另一筆車並讓歷史列卡住 closure。因此首次導入 v5 前，已有
-- vehicles 時必須先備份，再由操作者顯式設定以下 session 變數，
-- 授權清除 normalized 目錄後完整重爬。published_parts 不受 CASCADE
-- 影響，v_parts 在新 success 前仍保留上一版 snapshot。
SET @v5_was_missing := (SELECT COUNT(*) = 0 FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle_identity_v5');
SET @vehicle_rows := (SELECT COUNT(*) FROM vehicles);
SET @allow_vehicle_rebuild := COALESCE(@PARTSOUQ_ALLOW_V5_VEHICLE_REBUILD, 0);
SET @category_collision := (SELECT COUNT(*) FROM (
  SELECT vehicle_id, cid
  FROM categories
  WHERE cid IS NOT NULL AND cid <> ''
  GROUP BY vehicle_id, cid
  HAVING COUNT(*) > 1
) AS duplicate_category_identity);
DROP PROCEDURE IF EXISTS assert_partsouq_005_rebuild_authorized;
DELIMITER //
CREATE PROCEDURE assert_partsouq_005_rebuild_authorized()
BEGIN
  IF @v5_was_missing = 1 AND @vehicle_rows > 0 AND @allow_vehicle_rebuild <> 1 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'migration 005: set @PARTSOUQ_ALLOW_V5_VEHICLE_REBUILD=1 after backup';
  END IF;
  IF @category_collision > 0 AND NOT (@v5_was_missing = 1 AND @allow_vehicle_rebuild = 1) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'migration 005: duplicate category cid identity; manual merge required';
  END IF;
END//
DELIMITER ;
CALL assert_partsouq_005_rebuild_authorized();
DROP PROCEDURE assert_partsouq_005_rebuild_authorized;

-- 重建資料樹後，本月舊 success 不得讓 crawler 直接 early-exit。
-- 必須在 DELETE 前先落地；若 migration 在兩句之間中斷，重跑時
-- vehicles 已可能為空，仍不能讓舊 success 掩蓋已清空的 normalized tree。
-- 只要 v5 completion marker 尚未存在就重設本月 run；下次普通啟動
-- 會完整重爬，不需要額外 --fresh。
SET @sql := IF(@v5_was_missing = 1,
  'UPDATE crawl_runs SET status = ''error'', finished_at = NOW(),
     error_msg = ''vehicle v5 identity rebuild required''
   WHERE run_key = DATE_FORMAT(CURDATE(), ''%Y-%m'')',
  'SELECT ''current run invalidation not required''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := IF(@v5_was_missing = 1 AND @vehicle_rows > 0,
  'DELETE FROM vehicles',
  'SELECT ''vehicle rebuild not required''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND COLUMN_NAME='body_style');
SET @sql := IF(@col = 0,
  'ALTER TABLE vehicles ADD COLUMN body_style VARCHAR(256) NULL AFTER transmission',
  'SELECT ''vehicles.body_style already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @temp_col := (SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND COLUMN_NAME='identity_hash_v5');
SET @sql := IF(@v5_was_missing = 1 AND @temp_col = 0,
  'ALTER TABLE vehicles ADD COLUMN identity_hash_v5 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL AFTER identity_hash',
  'SELECT ''temporary v5 identity column not needed or already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := IF(@v5_was_missing = 1,
  'UPDATE vehicles SET identity_hash_v5 = SHA2(CONCAT(
     OCTET_LENGTH(CAST(model_id AS CHAR)), '':'', CAST(model_id AS CHAR),
     OCTET_LENGTH(COALESCE(model_code, '''')), '':'', COALESCE(model_code, ''''),
     OCTET_LENGTH(COALESCE(name, '''')), '':'', COALESCE(name, ''''),
     OCTET_LENGTH(COALESCE(description, '''')), '':'', COALESCE(description, ''''),
     OCTET_LENGTH(COALESCE(options, '''')), '':'', COALESCE(options, ''''),
     OCTET_LENGTH(COALESCE(prod_period, '''')), '':'', COALESCE(prod_period, ''''),
     OCTET_LENGTH(COALESCE(grade, '''')), '':'', COALESCE(grade, ''''),
     OCTET_LENGTH(COALESCE(market, '''')), '':'', COALESCE(market, ''''),
     OCTET_LENGTH(COALESCE(engine, '''')), '':'', COALESCE(engine, ''''),
     OCTET_LENGTH(COALESCE(transmission, '''')), '':'', COALESCE(transmission, ''''),
     OCTET_LENGTH(COALESCE(body_style, '''')), '':'', COALESCE(body_style, '''')
   ), 256)',
  'SELECT ''vehicle identity v5 already applied''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @vehicle_collision := IF(@v5_was_missing = 1,
  (SELECT COUNT(*) FROM (
    SELECT model_id, identity_hash_v5
    FROM vehicles
    GROUP BY model_id, identity_hash_v5
    HAVING COUNT(*) > 1
  ) AS duplicate_vehicle_identity),
  0);

DROP PROCEDURE IF EXISTS assert_partsouq_005_identity;
DELIMITER //
CREATE PROCEDURE assert_partsouq_005_identity()
BEGIN
  IF @vehicle_collision > 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'migration 005: duplicate vehicle v5 identity; manual merge required';
  END IF;
END//
DELIMITER ;
CALL assert_partsouq_005_identity();
DROP PROCEDURE assert_partsouq_005_identity;

SET @temp_idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle_identity_v5_stage');
SET @sql := IF(@v5_was_missing = 1 AND @temp_idx = 0,
  'ALTER TABLE vehicles ADD UNIQUE KEY uq_vehicle_identity_v5_stage (model_id, identity_hash_v5)',
  'SELECT ''temporary v5 identity index not needed or already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @old_idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle_identity');
SET @sql := IF(@v5_was_missing = 1 AND @old_idx > 0,
  'ALTER TABLE vehicles DROP INDEX uq_vehicle_identity',
  'SELECT ''legacy vehicle identity index already absent or v5 applied''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := IF(@v5_was_missing = 1,
  'UPDATE vehicles SET identity_hash = identity_hash_v5',
  'SELECT ''vehicle identity hash already v5''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- identity 公式變更後，非 success run 的舊 state/receipt/marker 無法
-- 再與 v5 key 閉合。只在首次套用 v5 時清除，保留 row_count 基準。
DELETE cs FROM crawl_state cs
JOIN crawl_runs cr ON cr.run_key = cs.run_key
WHERE @v5_was_missing = 1 AND (cr.status IS NULL OR cr.status <> 'success');
UPDATE groups_t g
JOIN crawl_runs cr ON cr.run_key = g.fetched_run_key
SET g.fetched_run_key = NULL, g.fetched_status = NULL
WHERE @v5_was_missing = 1 AND (cr.status IS NULL OR cr.status <> 'success');
UPDATE parts p
JOIN crawl_runs cr ON cr.id = p.seen_run_id
SET p.seen_run_id = NULL
WHERE @v5_was_missing = 1 AND (cr.status IS NULL OR cr.status <> 'success');

-- final index 最後建立：它也是 migration 完成 marker。若中途異常，
-- 重跑時 @v5_was_missing 仍為 1，會完成 state invalidation 再標記完成。
SET @final_idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle_identity_v5');
SET @sql := IF(@final_idx = 0,
  'ALTER TABLE vehicles ADD UNIQUE KEY uq_vehicle_identity_v5 (model_id, identity_hash)',
  'SELECT ''uq_vehicle_identity_v5 already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @cat_idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='categories'
    AND INDEX_NAME='uq_cat_cid');
SET @sql := IF(@cat_idx = 0,
  'ALTER TABLE categories ADD UNIQUE KEY uq_cat_cid (vehicle_id, cid)',
  'SELECT ''uq_cat_cid already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- cid 是有值時的穩定 identity。舊的 name-prefix UNIQUE 會把「不同 cid、
-- 同名」分類誤合併；降為普通索引，cid 缺失時由 repository 以完整 name
-- 查找後再 insert（單一 crawler lock 保證沒有同車並行寫入）。
SET @old_cat_idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='categories'
    AND INDEX_NAME='uq_cat');
SET @sql := IF(@old_cat_idx > 0,
  'ALTER TABLE categories DROP INDEX uq_cat',
  'SELECT ''legacy category name unique already absent''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @cat_name_idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='categories'
    AND INDEX_NAME='idx_cat_name');
SET @sql := IF(@cat_name_idx = 0,
  'ALTER TABLE categories ADD KEY idx_cat_name (vehicle_id, name(200))',
  'SELECT ''category name lookup index already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 中斷後重跑時，final index 已存在就可安全清掉 staging 物件。
SET @temp_idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle_identity_v5_stage');
SET @sql := IF(@temp_idx > 0,
  'ALTER TABLE vehicles DROP INDEX uq_vehicle_identity_v5_stage',
  'SELECT ''temporary v5 identity index already absent''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @temp_col := (SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND COLUMN_NAME='identity_hash_v5');
SET @sql := IF(@temp_col > 0,
  'ALTER TABLE vehicles DROP COLUMN identity_hash_v5',
  'SELECT ''temporary v5 identity column already absent''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
