import sqlite3

from core.trade_baseline_reset import MIGRATION_ID, apply_trade_baseline_reset


def _create_fixture(path):
    with sqlite3.connect(path) as conn:
        conn.executescript("""
            CREATE TABLE signals (id INTEGER PRIMARY KEY, symbol TEXT, result TEXT);
            CREATE TABLE signal_log (id INTEGER PRIMARY KEY, symbol TEXT, result TEXT);
            CREATE TABLE signal_stats (symbol TEXT PRIMARY KEY, total INTEGER);
            CREATE TABLE market_memory_snapshots (signal_id INTEGER PRIMARY KEY, outcome TEXT);
            CREATE TABLE market_memory_path (id INTEGER PRIMARY KEY, signal_id INTEGER, price REAL);
            CREATE TABLE observations (id INTEGER PRIMARY KEY, symbol TEXT, outcome TEXT);
            CREATE TABLE market_model (symbol TEXT PRIMARY KEY, best_setup TEXT);
            CREATE TABLE self_rules (
                id INTEGER PRIMARY KEY, rule TEXT, source TEXT, confirmed_by INTEGER,
                contradicted_by INTEGER, active INTEGER
            );
            CREATE TABLE knowledge (
                id INTEGER PRIMARY KEY, topic TEXT, content TEXT, source TEXT
            );
            CREATE TABLE news_market_context (id INTEGER PRIMARY KEY, payload TEXT);
            CREATE TABLE external_market_context (id INTEGER PRIMARY KEY, payload TEXT);
            CREATE TABLE user_memory (user_id INTEGER PRIMARY KEY, preferences TEXT);
            CREATE TABLE trade_executions (
                id INTEGER PRIMARY KEY, signal_id INTEGER, mode TEXT, status TEXT
            );

            INSERT INTO signals VALUES (1, 'BTCUSDT', 'sl');
            INSERT INTO signal_log VALUES (1, 'BTCUSDT', 'sl');
            INSERT INTO signal_stats VALUES ('BTCUSDT', 1);
            INSERT INTO market_memory_snapshots VALUES (1, 'sl');
            INSERT INTO market_memory_path VALUES (1, 1, 100.0);
            INSERT INTO observations VALUES (1, 'BTCUSDT', 'LOSS');
            INSERT INTO market_model VALUES ('BTCUSDT', 'bad legacy setup');
            INSERT INTO self_rules VALUES (1, 'curated SMC', 'smc_seed', 9, 3, 0);
            INSERT INTO self_rules VALUES (2, 'manual rule', 'manual', 4, 2, 0);
            INSERT INTO self_rules VALUES (3, 'bad learned rule', 'groq_trade_analysis', 8, 0, 1);
            INSERT INTO knowledge VALUES (1, 'market structure', 'keep', 'manual');
            INSERT INTO knowledge VALUES (2, 'reflection_BTCUSDT_sl', 'remove', 'self-reflection');
            INSERT INTO news_market_context VALUES (1, 'keep-news');
            INSERT INTO external_market_context VALUES (1, 'keep-external');
            INSERT INTO user_memory VALUES (7, 'keep-user');
            INSERT INTO trade_executions VALUES (1, 1, 'paper', 'PAPER_PENDING_ENTRY');
        """)


def test_reset_archives_trade_learning_and_preserves_configuration(tmp_path):
    db_path = tmp_path / "brain.db"
    _create_fixture(db_path)

    result = apply_trade_baseline_reset(str(db_path))

    assert result["applied"] is True
    with sqlite3.connect(db_path) as conn:
        for table in (
            "signals", "signal_log", "signal_stats", "market_memory_snapshots",
            "market_memory_path", "observations", "market_model", "trade_executions",
        ):
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0

        rules = conn.execute(
            "SELECT rule,confirmed_by,contradicted_by,active FROM self_rules ORDER BY id"
        ).fetchall()
        assert rules == [
            ("curated SMC", 0, 0, 1),
            ("manual rule", 0, 0, 1),
        ]
        assert conn.execute("SELECT topic FROM knowledge").fetchall() == [("market structure",)]
        assert conn.execute("SELECT payload FROM news_market_context").fetchone()[0] == "keep-news"
        assert conn.execute("SELECT payload FROM external_market_context").fetchone()[0] == "keep-external"
        assert conn.execute("SELECT preferences FROM user_memory").fetchone()[0] == "keep-user"
        assert conn.execute(
            "SELECT COUNT(*) FROM trade_reset_archive WHERE migration_id=?", (MIGRATION_ID,)
        ).fetchone()[0] >= 10

    second = apply_trade_baseline_reset(str(db_path))
    assert second["applied"] is False
    assert second["already_applied"] is True


def test_reset_is_safe_for_database_with_missing_optional_tables(tmp_path):
    db_path = tmp_path / "brain.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT)")
        conn.execute("INSERT INTO signals VALUES (1, 'tp1')")

    result = apply_trade_baseline_reset(str(db_path))

    assert result["applied"] is True
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 0


def test_reset_refuses_to_remove_active_live_execution(tmp_path):
    db_path = tmp_path / "brain.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT);
            CREATE TABLE trade_executions (
                id INTEGER PRIMARY KEY, signal_id INTEGER, mode TEXT, status TEXT
            );
            INSERT INTO signals VALUES (1, 'pending');
            INSERT INTO trade_executions VALUES (1, 1, 'live', 'PROTECTED');
        """)

    result = apply_trade_baseline_reset(str(db_path))

    assert result["applied"] is False
    assert result["blocked"] is True
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 1
        marker_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='system_migrations'"
        ).fetchone()
        assert marker_table is None
