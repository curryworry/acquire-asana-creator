import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api.dashboard_service import bq_context, ensure_control_tables


def main() -> None:
    client, project_id, dataset = bq_context()
    ensure_control_tables(client, project_id, dataset)
    print(f"Ensured control tables in {project_id}.{dataset}")


if __name__ == "__main__":
    main()
