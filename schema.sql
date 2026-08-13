-- PartSouq crawler schema
-- Monthly full-crawl of https://partsouq.com/en/catalog/genuine

CREATE DATABASE IF NOT EXISTS partsouq_crawler CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE partsouq_crawler;

-- 品牌 (Brand)
CREATE TABLE IF NOT EXISTS brands (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(64) NOT NULL,          -- TOYOTA, Lexus, ...
  code        VARCHAR(64) NULL,              -- TOYOTA00
  url         VARCHAR(512) NULL,
  UNIQUE KEY uq_brand_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 車系/型號 (Model, from locate page accordion)
CREATE TABLE IF NOT EXISTS models (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  brand_id    INT NOT NULL,
  name        VARCHAR(128) NOT NULL,         -- 4RUNNER, COROLLA, ...
  ssd         TEXT NULL,                     -- session token for pick page
  url         VARCHAR(1024) NULL,
  fetched_at  DATETIME NULL,
  UNIQUE KEY uq_model (brand_id, name),
  CONSTRAINT fk_model_brand FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 具體車款 (Vehicle, from pick page Specifications table)
CREATE TABLE IF NOT EXISTS vehicles (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  model_id    INT NOT NULL,
  identity_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, -- v5 穩定規格 SHA256；不含 session token
  name        VARCHAR(256) NOT NULL DEFAULT '',  -- Name: ALPHARD/VELLFIRE/HV
  description VARCHAR(512) NULL,             -- Description: AGH3#,AYH30,GGH3#
  model_code  VARCHAR(128) NOT NULL DEFAULT '',  -- Model: AGH30W-NFXGK
  options     VARCHAR(512) NULL,             -- Options: ATM,MTM: ...
  prod_period VARCHAR(64) NULL,              -- Prod Period: 01.2015 - ...
  grade       VARCHAR(256) NULL,
  market      VARCHAR(128) NULL,
  engine      VARCHAR(256) NULL,
  transmission VARCHAR(256) NULL,
  body_style  VARCHAR(256) NULL,
  ssd         TEXT NULL,                     -- vehicle session token
  vid         VARCHAR(32) NULL,              -- vid param
  url         VARCHAR(1024) NULL,
  fetched_at  DATETIME NULL,
  UNIQUE KEY uq_vehicle_identity_v5 (model_id, identity_hash),
  CONSTRAINT fk_vehicle_model FOREIGN KEY (model_id) REFERENCES models(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 零件大分類 (Main category: ENGINE/FUEL/TOOL, POWER TRAIN/CHASSIS, ...)
CREATE TABLE IF NOT EXISTS categories (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  vehicle_id  INT NOT NULL,
  name        VARCHAR(256) NOT NULL,
  cid         VARCHAR(32) NULL,              -- cid param
  fetched_at  DATETIME NULL,
  KEY idx_cat_name (vehicle_id, name(200)),
  UNIQUE KEY uq_cat_cid (vehicle_id, cid),
  CONSTRAINT fk_cat_vehicle FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 零件中/小分類 (Group: 0901 STANDARD TOOL, 1101 PARTIAL ENGINE ASSEMBLY, ...)
CREATE TABLE IF NOT EXISTS groups_t (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  category_id INT NOT NULL,
  code        VARCHAR(16) NOT NULL DEFAULT '',  -- 0901, 1101...
  name        VARCHAR(256) NULL,             -- STANDARD TOOL
  uid         VARCHAR(32) NULL,              -- uid param
  url         VARCHAR(1024) NULL,
  fetched_at  DATETIME NULL,
  fetched_run_key VARCHAR(32) NULL,          -- 最後一次抓取零件的 run_key（group terminal state，F1b）
  fetched_status VARCHAR(16) NULL,           -- done / not_found（F5 receipt；HTTP 200 零解析一律視為異常不寫 receipt）
  fetched_row_count INT DEFAULT 0,           -- 本組零件筆數（F5 receipt，content hash 基礎）
  verified_row_count INT NOT NULL DEFAULT 0, -- 歷次 done 的最高筆數；縮水偵測基準，只升不降
  UNIQUE KEY uq_group (category_id, code),
  CONSTRAINT fk_group_cat FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 零件 (Part, from unit page table)
CREATE TABLE IF NOT EXISTS parts (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  group_id    INT NOT NULL,
  part_number VARCHAR(64) NOT NULL,          -- Number: 190000V200
  name        VARCHAR(512) NULL,             -- Name: ENGINE ASSY, PARTIAL
  code        VARCHAR(64) NULL,              -- Code: 11000
  note        TEXT NULL,                     -- Note
  quantity    VARCHAR(16) NULL,              -- Quantity: 01
  range_str   VARCHAR(64) NOT NULL DEFAULT '',  -- Range: 01.2015 - 01.2018
  url         VARCHAR(1024) NULL,
  seen_run_id BIGINT NULL,                    -- 最近一次完整抓到此列的 logical run id
  created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_part (group_id, part_number, range_str),
  KEY idx_part_number (part_number),
  KEY idx_part_name (name(200)),
  KEY idx_part_updated (updated_at),
  KEY idx_part_seen_run (seen_run_id),
  CONSTRAINT fk_part_group FOREIGN KEY (group_id) REFERENCES groups_t(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 最近一次完整 success 的不可變、反正規化 current snapshot。
-- normalized tables 可讓 failed/partial attempt 繼續 upsert；v_parts 只讀
-- 本表，因此不會在未完成 attempt 中途改變。成功收尾時以同一交易
-- upsert 本次列並刪除過期列；失敗 rollback 後仍保留上一版。
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

-- 爬蟲狀態 (斷點續爬)
CREATE TABLE IF NOT EXISTS crawl_state (
  id           INT AUTO_INCREMENT PRIMARY KEY,
  run_key      VARCHAR(32) NULL,             -- 當月 run 標記（例如 '2026-08'）；NULL=相容模式
  scope        VARCHAR(32) NOT NULL,         -- brand / model / vehicle / category / group / part
  scope_key    VARCHAR(256) NOT NULL,        -- unique key within scope
  status       VARCHAR(16) NOT NULL,         -- pending / done / error
  error_msg    TEXT NULL,
  updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_state_run (run_key, scope, scope_key),
  KEY idx_state_run (run_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 爬蟲運行記錄
CREATE TABLE IF NOT EXISTS crawl_runs (
  id           INT AUTO_INCREMENT PRIMARY KEY,
  run_key      VARCHAR(32) NULL,             -- 當月 run 標記（例如 '2026-08'）
  started_at   DATETIME NOT NULL,
  finished_at  DATETIME NULL,
  status       VARCHAR(16) NULL,             -- running / success / error
  brands_ok    INT DEFAULT 0,
  models_ok    INT DEFAULT 0,
  vehicles_ok  INT DEFAULT 0,
  groups_ok    INT DEFAULT 0,
  parts_ok     INT DEFAULT 0,
  parts_new    INT DEFAULT 0,
  error_msg    TEXT NULL,
  UNIQUE KEY uq_run_key (run_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 「現存」語意：只讀最近一次完整 success 交易建立的 snapshot。
CREATE OR REPLACE VIEW v_parts AS
SELECT
  brand, model, vehicle_name, vehicle_code, prod_period,
  part_name, part_number, category_main, category_group, group_code,
  part_range, note, quantity, code
FROM published_parts;
