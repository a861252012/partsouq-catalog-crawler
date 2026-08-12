-- PartSouq crawler — schema migration 002：月次 run 隔離
-- 目的：讓「每月全站爬取」能真正每月重跑一次，而不是第一次成功後
--      永遠退出（P0 問題）。
--
-- 變更：
--   1. crawl_runs 加 run_key（每月唯一的標記，例如 '2026-08'）
--   2. crawl_state 加 run_key（done 狀態按 run 隔離，跨月不共享）
--
-- 可重複執行（idempotent）。現有資料列 run_key 補上 NULL（相容）：
--   - 既有 run 的 run_key 為 NULL，視為「通用」run
--   - 新 run 一律帶當月 run_key
--
-- 用法：mysql ... partsouq_crawler < migrations/002_monthly_run_isolation.sql
--       mysql ... partsouq_crawler_test < migrations/002_monthly_run_isolation.sql
-- 注意：不內含 USE 陳述；DB 由命令列指定，資訊架構檢查用 DATABASE() 對齊。

-- crawl_runs.run_key
SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='crawl_runs'
    AND COLUMN_NAME='run_key');
SET @sql := IF(@col = 0,
  'ALTER TABLE crawl_runs ADD COLUMN run_key VARCHAR(32) NULL AFTER id',
  'SELECT ''crawl_runs.run_key already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='crawl_runs'
    AND INDEX_NAME='uq_run_key');
SET @sql := IF(@idx = 0,
  'ALTER TABLE crawl_runs ADD UNIQUE KEY uq_run_key (run_key)',
  'SELECT ''uq_run_key already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- crawl_state.run_key
SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='crawl_state'
    AND COLUMN_NAME='run_key');
SET @sql := IF(@col = 0,
  'ALTER TABLE crawl_state ADD COLUMN run_key VARCHAR(32) NULL AFTER id',
  'SELECT ''crawl_state.run_key already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @idx := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='crawl_state'
    AND INDEX_NAME='idx_state_run');
SET @sql := IF(@idx = 0,
  'ALTER TABLE crawl_state ADD INDEX idx_state_run (run_key)',
  'SELECT ''idx_state_run already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- crawl_state 唯一鍵：改為 (run_key, scope, scope_key)。
-- 這樣每個 run（每月）有自己的 done 集合，跨月不共享（P0 修復核心）。
-- 既有資料 run_key 為 NULL（允許重複），不影響。
-- 先確認新的唯一鍵不存在才改，避免重複執行報錯。
SET @uk := (SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='crawl_state'
    AND INDEX_NAME='uq_state_run');
SET @sql := IF(@uk = 0,
  'ALTER TABLE crawl_state DROP INDEX uq_state, '
  'ADD UNIQUE KEY uq_state_run (run_key, scope, scope_key)',
  'SELECT ''uq_state_run already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
