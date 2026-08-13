-- PartSouq crawler — schema migration 006：group row-count high-water
--
-- fetched_row_count 只代表最後一次 receipt，逐月小幅縮水時會跟著下降，
-- 無法作為歷史基準。verified_row_count 只在 status='done' 時取較大值，
-- not_found 與較小的成功結果都不會降低它。
--
-- 可重複執行；既有 done receipt 會作為初始 high-water。

SET SESSION lock_wait_timeout = 30;
SET SESSION innodb_lock_wait_timeout = 30;

SET @col := (SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='groups_t'
    AND COLUMN_NAME='verified_row_count');
SET @sql := IF(@col = 0,
  'ALTER TABLE groups_t ADD COLUMN verified_row_count INT NOT NULL DEFAULT 0 AFTER fetched_row_count',
  'SELECT ''groups_t.verified_row_count already exists''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

UPDATE groups_t
SET verified_row_count = COALESCE(fetched_row_count, 0)
WHERE fetched_status = 'done'
  AND COALESCE(fetched_row_count, 0) > verified_row_count;
