"""Run canonical APEX V3 State and Live Memory migrations fail-closed."""
from __future__ import annotations

from apex.config.settings import ApexConfig
from apex.db.connection import connect_memory, connect_state
from apex.db.integrity import check_integrity
from apex.db.memory_db import migrate_memory
from apex.db.state_db import migrate_state


def main() -> None:
    config = ApexConfig.from_env()
    state = connect_state(config)
    try:
        state_versions = migrate_state(state)
        check_integrity(state)
    finally:
        state.close()

    memory = connect_memory(config)
    try:
        memory_versions = migrate_memory(memory)
        check_integrity(memory)
    finally:
        memory.close()

    print({"state": state_versions, "memory": memory_versions})


if __name__ == "__main__":
    main()
