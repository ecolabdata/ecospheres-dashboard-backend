"""add Organization.type

Revision ID: b99ee027d14f
Revises: af780a5cffbe
Create Date: 2024-12-05 15:31:18.152913

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b99ee027d14f"
down_revision: Union[str, None] = "ccdd23b19763"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("organizations", sa.Column("type", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("organizations", "type")
