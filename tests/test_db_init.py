from testmon import db as testmon_db


def test_init_tables_twice_is_a_no_op(tmp_path):
    database = testmon_db.DB(str(tmp_path / ".testmondata"))
    database.init_tables()  # a second process initialising the same file
    assert database.con.execute("PRAGMA user_version").fetchone()[0] == (
        database.version_compatibility()
    )

