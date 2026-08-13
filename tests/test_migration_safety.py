import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MIGRATION_001 = (ROOT / "migrations" / "001_fix_nullable_unique_keys.sql").read_text()
MIGRATION_005 = (ROOT / "migrations" / "005_vehicle_identity_v5_and_category_cid.sql").read_text()


def _code(sql):
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def test_001_preflights_every_normalized_unique_key_before_writes():
    sql = MIGRATION_001
    compact = " ".join(_code(sql).split())
    assert (
        "GROUP BY category_id, normalized_code HAVING COUNT(*) > 1 AND "
        "SUM(CASE WHEN code IS NULL THEN 1 ELSE 0 END) > 0"
    ) in compact
    assert (
        "GROUP BY group_id, part_number, normalized_range HAVING COUNT(*) > 1 AND "
        "SUM(CASE WHEN range_str IS NULL THEN 1 ELSE 0 END) > 0"
    ) in compact
    assert (
        "LEFT(COALESCE(name, ''), 120) AS normalized_name_prefix FROM vehicles "
        "GROUP BY model_id, normalized_model_code, normalized_name_prefix "
        "HAVING COUNT(*) > 1"
    ) in compact

    assertion = sql.index("CALL assert_partsouq_001_normalization()")
    first_write = sql.index("'UPDATE groups_t SET code")
    first_alter = sql.index("'ALTER TABLE groups_t MODIFY code")
    assert assertion < first_write < first_alter
    assert sql.index("'UPDATE parts SET range_str") < first_alter
    assert sql.index("'UPDATE vehicles SET name") < first_alter
    assert sql.index("'UPDATE vehicles SET model_code") < first_alter
    for statement in (
        "UPDATE groups_t SET code = '''' WHERE code IS NULL",
        "UPDATE parts SET range_str = '''' WHERE range_str IS NULL",
        "UPDATE vehicles SET name = '''' WHERE name IS NULL",
        "UPDATE vehicles SET model_code = '''' WHERE model_code IS NULL",
        "ALTER TABLE groups_t MODIFY code VARCHAR(16) NOT NULL DEFAULT ''''",
        "ALTER TABLE parts MODIFY range_str VARCHAR(64) NOT NULL DEFAULT ''''",
        "ALTER TABLE vehicles MODIFY name VARCHAR(256) NOT NULL DEFAULT ''''",
        "ALTER TABLE vehicles MODIFY model_code VARCHAR(128) NOT NULL DEFAULT ''''",
    ):
        assert statement in compact


def test_001_is_guarded_for_idempotent_reruns_and_bounds_lock_waits():
    sql = MIGRATION_001
    assert "SET SESSION lock_wait_timeout = 30" in sql
    assert "SET SESSION innodb_lock_wait_timeout = 30" in sql
    for marker in (
        "@groups_code_nullable > 0",
        "@parts_range_nullable > 0",
        "@vehicles_name_nullable > 0",
        "@vehicles_model_code_nullable > 0",
        "@idx_exists = 0",
    ):
        assert marker in sql
    assert "@required_column_count <> 4" in sql
    assert sql.index("CALL assert_partsouq_001_schema()") < sql.index("SET @groups_code_collision")
    assert sql.count("SIGNAL SQLSTATE '45000'") == 4


def test_005_invalidates_all_successes_before_tree_deletion_without_timezone():
    sql = MIGRATION_005
    code = _code(sql)
    assert "CURDATE" not in code
    assert "DATE_FORMAT" not in code
    authorization = sql.index("CALL assert_partsouq_005_rebuild_authorized()")
    invalidation = sql.index("UPDATE crawl_runs SET status = ''error''")
    invalidation_end = sql.index("PREPARE stmt FROM @sql", invalidation)
    invalidation_sql = sql[invalidation:invalidation_end]
    assert "WHERE status = ''success''" in invalidation_sql
    assert "LIMIT" not in invalidation_sql.upper()
    first_delete = sql.index("DELETE FROM parts ORDER BY id LIMIT 1000")
    assert authorization < invalidation < sql.index("COMMIT;", invalidation) < first_delete


def test_005_rebuild_is_restartable_batched_child_to_parent():
    sql = MIGRATION_005
    code = _code(sql)
    deletes = [
        "DELETE FROM crawl_state ORDER BY id LIMIT 1000",
        "DELETE FROM parts ORDER BY id LIMIT 1000",
        "DELETE FROM groups_t ORDER BY id LIMIT 1000",
        "DELETE FROM categories ORDER BY id LIMIT 1000",
        "DELETE FROM vehicles ORDER BY id LIMIT 1000",
    ]
    positions = [sql.index(statement) for statement in deletes]
    assert positions == sorted(positions)
    for table in ("crawl_state", "parts", "groups_t", "categories", "vehicles"):
        statements = re.findall(
            rf"DELETE\s+FROM\s+{table}\b[^;]*;", code, re.IGNORECASE | re.DOTALL
        )
        assert len(statements) == 1
        assert re.search(r"ORDER\s+BY\s+id\s+LIMIT\s+1000\s*;", statements[0])
    assert sql.count("SET deleted_rows = ROW_COUNT()") == 5
    assert sql.count("COMMIT;") >= 6
    assert code.count("WHILE deleted_rows > 0 DO") == 5
    assert "IF @v5_was_missing = 1 THEN" in sql
    assert sql.index("CALL rebuild_partsouq_005_vehicle_tree()") < sql.index(
        "ALTER TABLE vehicles ADD UNIQUE KEY uq_vehicle_identity_v5 ("
    )


def test_005_preserves_snapshot_and_bounds_lock_waits():
    sql = MIGRATION_005
    assert "SET SESSION lock_wait_timeout = 30" in sql
    assert "SET SESSION innodb_lock_wait_timeout = 30" in sql
    assert not re.search(
        r"(?:DELETE\s+FROM|TRUNCATE\s+TABLE|DROP\s+TABLE|ALTER\s+TABLE|"
        r"UPDATE|INSERT\s+INTO)\s+published_parts\b",
        _code(sql),
        re.IGNORECASE,
    )
    assert "published_parts" in sql


def test_005_validates_completion_indexes_and_rerun_does_not_resolve_temp_column():
    sql = MIGRATION_005
    compact = " ".join(_code(sql).split())
    assert "@v5_index_rows > 0 AND @v5_index_valid = 0" in sql
    assert "@cat_index_rows > 0 AND @cat_index_valid = 0" in sql
    assert ("GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) = 'model_id,identity_hash'") in compact
    assert ("GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) = 'vehicle_id,cid'") in compact
    collision = compact.index("SELECT COUNT(*) INTO @vehicle_collision")
    prepare = compact.index("PREPARE stmt FROM @sql", collision)
    assert collision < prepare


def test_every_migration_bounds_metadata_and_row_lock_waits():
    for path in sorted((ROOT / "migrations").glob("*.sql")):
        sql = path.read_text()
        assert "SET SESSION lock_wait_timeout = 30" in sql, path.name
        assert "SET SESSION innodb_lock_wait_timeout = 30" in sql, path.name


def test_readme_runs_006_after_005_and_verifies_column():
    readme = (ROOT / "README.md").read_text()
    migration_005 = "migrations/005_vehicle_identity_v5_and_category_cid.sql"
    migration_006 = "migrations/006_group_high_water.sql"
    assert (ROOT / migration_006).is_file()
    assert readme.index(migration_005) < readme.index(migration_006)
    assert "SHOW COLUMNS FROM groups_t LIKE 'verified_row_count'" in readme
