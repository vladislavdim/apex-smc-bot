"""Read-only inspection of canonical APEX V3 State DB."""
from apex.config.settings import ApexConfig
from apex.db.connection import connect_state


def main() -> None:
    conn = connect_state(ApexConfig.from_env(), read_only=True)
    try:
        tables = [str(row[0]) for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )]
        for table in tables:
            print(table)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
