-- PartSouq crawler — schema migration 003：group terminal state receipt 欄位
-- 目的：groups_t 新增 F1b/F5 的「本 run 已抓完」receipt 三欄，
--      讓續爬/重試可以依 terminal state 跳過已完成的組。
--
-- 變更：groups_t 加
--   1. fetched_run_key   VARCHAR(32) NULL  -- 最後一次抓取零件的 run_key
--   2. fetched_status    VARCHAR(16) NULL  -- done / not_found
--   3. fetched_row_count INT DEFAULT 0     -- 本組零件筆數
--
-- 可重複執行（idempotent）：每個欄位先查 information_schema，已存在
-- 則跳過（正式/測試庫已手動加過欄位時執行本 migration 是 no-op）。
--
-- 用法：mysql ... partsouq_crawler < migrations/003_group_receipt_columns.sql
--       mysql ... partsouq_crawler_test < migrations/003_group_receipt_columns.sql

SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='groups_t'
    AND COLUMN_NAME='fetched_run_key');
SET @sql := IF(@col = 0,
  'ALTER TABLE groups_t ADD COLUMN fetched_run_key VARCHAR(32) NULL AFTER fetched_at',
  'SELECT ''groups_t.fetched_run_key already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='groups_t'
    AND COLUMN_NAME='fetched_status');
SET @sql := IF(@col = 0,
  'ALTER TABLE groups_t ADD COLUMN fetched_status VARCHAR(16) NULL AFTER fetched_run_key',
  'SELECT ''groups_t.fetched_status already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='groups_t'
    AND COLUMN_NAME='fetched_row_count');
SET @sql := IF(@col = 0,
  'ALTER TABLE groups_t ADD COLUMN fetched_row_count INT DEFAULT 0 AFTER fetched_status',
  'SELECT ''groups_t.fetched_row_count already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
