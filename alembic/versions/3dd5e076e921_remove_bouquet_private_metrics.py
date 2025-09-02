"""Remove Bouquet private metrics

Revision ID: 3dd5e076e921
Revises: cd7258fee3ae
Create Date: 2025-07-03 10:37:18.319196

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3dd5e076e921"
down_revision: Union[str, None] = "cd7258fee3ae"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    metrics = [
        "nb_bouquets",
        "nb_datasets_in_bouquets",
        "nb_datasets_external_in_bouquets",
        "nb_factors_in_bouquets",
        "nb_factors_missing_in_bouquets",
        "nb_factors_not_available_in_bouquets",
    ]
    for metric in metrics:
        op.execute(f"DELETE FROM metrics WHERE measurement = '{metric}'")


def downgrade() -> None:
    pass
