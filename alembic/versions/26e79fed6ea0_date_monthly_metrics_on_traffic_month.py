"""date monthly metrics on traffic month

Monthly data.gouv.fr metrics were stored on the first day of the month following
the traffic month, under `*_last_month` measurements. Store them on the first day
of the traffic month instead, under `*_monthly` measurements.

Revision ID: 26e79fed6ea0
Revises: 4f87e4cf0412
Create Date: 2026-09-25 15:12:41.000000

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "26e79fed6ea0"
down_revision: Union[str, None] = "4f87e4cf0412"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# table -> [(old measurement, new measurement)]
RENAMES = {
    "datasets_metrics": [
        ("nb_visits_last_month", "nb_visits_monthly"),
        ("nb_downloads_resources_last_month", "nb_downloads_resources_monthly"),
    ],
    "metrics": [
        ("nb_visits_datasets_last_month", "nb_visits_datasets_monthly"),
        ("nb_downloads_resources_last_month", "nb_downloads_resources_monthly"),
    ],
}


def upgrade() -> None:
    for table, renames in RENAMES.items():
        for old, new in renames:
            op.execute(
                f"UPDATE {table} SET measurement = '{new}', "
                f"date = (date - interval '1 month')::date WHERE measurement = '{old}'"
            )


def downgrade() -> None:
    for table, renames in RENAMES.items():
        for old, new in renames:
            op.execute(
                f"UPDATE {table} SET measurement = '{old}', "
                f"date = (date + interval '1 month')::date WHERE measurement = '{new}'"
            )
