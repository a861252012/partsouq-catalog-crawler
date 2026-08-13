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
--
-- 避免 migration 無限等待 crawler 或其他 session 的 metadata / row lock。
-- 逾時代表仍有 writer 或長交易；停止 migration、清查後直接重跑。
SET SESSION lock_wait_timeout = 30;
SET SESSION innodb_lock_wait_timeout = 30;

SET @v5_index_rows := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle_identity_v5');
SET @v5_index_valid := (SELECT IF(
  COUNT(*) = 2 AND MAX(NON_UNIQUE) = 0 AND
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) = 'model_id,identity_hash',
  1, 0)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle_identity_v5');
SET @v5_was_missing := (@v5_index_valid = 0);
SET @cat_index_rows := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='categories'
    AND INDEX_NAME='uq_cat_cid');
SET @cat_index_valid := (SELECT IF(
  COUNT(*) = 2 AND MAX(NON_UNIQUE) = 0 AND
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) = 'vehicle_id,cid',
  1, 0)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='categories'
    AND INDEX_NAME='uq_cat_cid');
SET @vehicle_rows := (SELECT COUNT(*) FROM vehicles);
SET @allow_vehicle_rebuild := COALESCE(@PARTSOUQ_ALLOW_V5_VEHICLE_REBUILD, 0);
SET @category_collision := (SELECT COUNT(*) FROM (
  SELECT vehicle_id, cid
  FROM categories
  WHERE cid IS NOT NULL
  GROUP BY vehicle_id, cid
  HAVING COUNT(*) > 1
) AS duplicate_category_identity);
DROP PROCEDURE IF EXISTS assert_partsouq_005_rebuild_authorized;
DELIMITER //
CREATE PROCEDURE assert_partsouq_005_rebuild_authorized()
BEGIN
  IF @v5_index_rows > 0 AND @v5_index_valid = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'migration 005: uq_vehicle_identity_v5 definition mismatch';
  END IF;
  IF @cat_index_rows > 0 AND @cat_index_valid = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'migration 005: uq_cat_cid definition mismatch';
  END IF;
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

-- 重建資料樹後，任何舊 success 都不得讓 crawler 直接 early-exit。
-- 必須在 DELETE 前先落地；若 migration 在兩句之間中斷，重跑時
-- vehicles 已可能為空，仍不能讓舊 success 掩蓋已清空的 normalized tree。
-- 不從資料庫日期函式推導月份，避免 DB 與 crawler timezone 在月界不同。
-- 只要 v5 completion marker 尚未存在，就讓所有既有 success 失效；
-- published_parts 不變，所以上一份已發布 snapshot 仍可讀。
SET @sql := IF(@v5_was_missing = 1,
  'UPDATE crawl_runs SET status = ''error'', finished_at = NOW(),
     error_msg = ''vehicle v5 identity rebuild required''
   WHERE status = ''success''',
  'SELECT ''success run invalidation not required''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
COMMIT;

-- 大型 tree 不使用單句 cascading DELETE。依 child -> parent 每批 1000 列
-- 刪除並 COMMIT；lock timeout 或連線中斷時，已完成批次會保留，重跑會從
-- 剩餘資料繼續。published_parts 沒有 FK 指向 normalized tree，不會被刪除。
DROP PROCEDURE IF EXISTS rebuild_partsouq_005_vehicle_tree;
DELIMITER //
CREATE PROCEDURE rebuild_partsouq_005_vehicle_tree()
BEGIN
  DECLARE deleted_rows INT DEFAULT 0;

  IF @v5_was_missing = 1 THEN
    SET deleted_rows = 1;
    WHILE deleted_rows > 0 DO
      DELETE FROM crawl_state ORDER BY id LIMIT 1000;
      SET deleted_rows = ROW_COUNT();
      COMMIT;
    END WHILE;

    SET deleted_rows = 1;
    WHILE deleted_rows > 0 DO
      DELETE FROM parts ORDER BY id LIMIT 1000;
      SET deleted_rows = ROW_COUNT();
      COMMIT;
    END WHILE;

    SET deleted_rows = 1;
    WHILE deleted_rows > 0 DO
      DELETE FROM groups_t ORDER BY id LIMIT 1000;
      SET deleted_rows = ROW_COUNT();
      COMMIT;
    END WHILE;

    SET deleted_rows = 1;
    WHILE deleted_rows > 0 DO
      DELETE FROM categories ORDER BY id LIMIT 1000;
      SET deleted_rows = ROW_COUNT();
      COMMIT;
    END WHILE;

    SET deleted_rows = 1;
    WHILE deleted_rows > 0 DO
      DELETE FROM vehicles ORDER BY id LIMIT 1000;
      SET deleted_rows = ROW_COUNT();
      COMMIT;
    END WHILE;
  END IF;
END//
DELIMITER ;
CALL rebuild_partsouq_005_vehicle_tree();
DROP PROCEDURE rebuild_partsouq_005_vehicle_tree;

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

-- 用 dynamic SQL 隔離 staging 欄位。完成後重跑時該欄位已刪除；即使
-- IF 條件為 false，直接寫在 expression 裡仍可能於 prepare 階段解析失敗。
SET @sql := IF(@v5_was_missing = 1,
  'SELECT COUNT(*) INTO @vehicle_collision FROM (
    SELECT model_id, identity_hash_v5
    FROM vehicles
    GROUP BY model_id, identity_hash_v5
    HAVING COUNT(*) > 1
  ) AS duplicate_vehicle_identity',
  'SELECT 0 INTO @vehicle_collision');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

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

-- identity 公式變更後，舊 state/receipt/marker 無法再與 v5 key 閉合。
-- crawl_state 已在 restartable rebuild procedure 分批清除；group receipt 與
-- part membership 隨 normalized tree 一起分批刪除，不需要再做全表 UPDATE。

-- final index 最後建立：它也是 migration 完成 marker。若中途異常，
-- 重跑時 @v5_was_missing 仍為 1，會完成 state invalidation 再標記完成。
SET @final_idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='vehicles'
    AND INDEX_NAME='uq_vehicle_identity_v5');
SET @sql := IF(@final_idx = 0,
  'ALTER TABLE vehicles ADD UNIQUE KEY uq_vehicle_identity_v5 (model_id, identity_hash)',
  'SELECT ''uq_vehicle_identity_v5 already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @cat_idx := @cat_index_rows;
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
