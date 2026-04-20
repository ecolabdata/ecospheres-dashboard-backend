"""add_stats_period_column

Revision ID: 4f87e4cf0412
Revises: e80f9232f15c
Create Date: 2026-04-20 14:38:04.783876

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4f87e4cf0412'
down_revision: Union[str, None] = 'e80f9232f15c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    statsperiod = sa.Enum('day', 'month', name='statsperiod')
    statsperiod.create(op.get_bind())
    op.add_column('stats', sa.Column('period', statsperiod, nullable=False, server_default='day'))


def downgrade() -> None:
    op.drop_column('stats', 'period')
    sa.Enum(name='statsperiod').drop(op.get_bind())
