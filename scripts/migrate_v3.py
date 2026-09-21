"""Run only canonical V3 State and Memory schema migrations."""
from apex.config.settings import ApexConfig
from apex.db.connection import connect_memory, connect_state
from apex.db.memory_db import migrate_memory
from apex.db.state_db import migrate_state


def main() -> None:
    config = ApexConfig.from_env()
    state = connect_state(config)
    memory = connect_memory(config)
    try:
        print({"state": migrate_state(state), "memory": migrate_memory(memory)})
    finally:
        state.close()
        memory.close()


if __name__ == "__main__":
    main()
