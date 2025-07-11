"""Add computed fields

Revision ID: 7d0e6fd2acf7
Revises: 3dd5e076e921
Create Date: 2025-07-11 17:57:54.858161

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7d0e6fd2acf7"
down_revision: Union[str, None] = "3dd5e076e921"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("catalog", "description__length")
    op.add_column("catalog", sa.Column("contact_points__first__email", sa.String(), nullable=True))
    op.add_column("catalog", sa.Column("contact_points__first__name", sa.String(), nullable=True))
    op.add_column("catalog", sa.Column("description__length__bin", sa.Integer(), nullable=False))
    op.add_column(
        "catalog", sa.Column("description__length__bin_label", sa.String(), nullable=False)
    )
    op.add_column("catalog", sa.Column("harvest__created_at__year", sa.Integer(), nullable=True))
    op.add_column("catalog", sa.Column("harvest__modified_at__year", sa.Integer(), nullable=True))
    op.add_column("catalog", sa.Column("has_harvest", sa.Boolean(), nullable=False))
    op.add_column("catalog", sa.Column("quality__score", sa.Float(), nullable=False))
    op.add_column("catalog", sa.Column("quality__score__bin", sa.Integer(), nullable=False))
    op.add_column("catalog", sa.Column("quality__score__bin_label", sa.String(), nullable=False))
    op.add_column("catalog", sa.Column("spatial__coordinates", sa.String(), nullable=True))
    op.add_column("catalog", sa.Column("temporal_coverage__range", sa.String(), nullable=True))
    op.add_column("resources", sa.Column("schema__exists", sa.Boolean(), nullable=False))
    op.add_column("resources", sa.Column("schema__name", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("catalog", "contact_points__first__email")
    op.drop_column("catalog", "contact_points__first__name")
    op.drop_column("catalog", "description__length__bin")
    op.drop_column("catalog", "description__length__bin_label")
    op.drop_column("catalog", "harvest__created_at__year")
    op.drop_column("catalog", "harvest__modified_at__year")
    op.drop_column("catalog", "has_harvest")
    op.drop_column("catalog", "quality__score")
    op.drop_column("catalog", "quality__score__bin")
    op.drop_column("catalog", "quality__score__bin_label")
    op.drop_column("catalog", "spatial__coordinates")
    op.drop_column("catalog", "temporal_coverage__range")
    op.drop_column("resources", "schema__name")
    op.drop_column("resources", "schema__exists")
    op.add_column(
        "catalog",
        sa.Column("description__length", sa.INTEGER(), autoincrement=False, nullable=False),
    )
