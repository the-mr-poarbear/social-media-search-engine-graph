"""add is_private and discovered_by to users

Revision ID: eeb0761df8aa
Revises: 280e2fd3e79f
Create Date: 2026-08-02 18:34:22.531947

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'eeb0761df8aa'
down_revision: Union[str, Sequence[str], None] = '280e2fd3e79f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('users', sa.Column('is_private', sa.Boolean(), nullable=True))
    op.add_column('users', sa.Column('discovered_by', sa.BigInteger(), nullable=True))
    op.create_foreign_key('fk_users_discovered_by_users', 'users', 'users', ['discovered_by'], ['id'])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('fk_users_discovered_by_users', 'users', type_='foreignkey')
    op.drop_column('users', 'discovered_by')
    op.drop_column('users', 'is_private')