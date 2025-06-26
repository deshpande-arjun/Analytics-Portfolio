"""initial tables"""
from alembic import op
import sqlalchemy as sa

def upgrade():
    op.create_table(
        'fundamental_overview',
        sa.Column('ticker', sa.Text(), primary_key=True),
        sa.Column('data', sa.Text()),
        sa.Column('pulled_at', sa.TIMESTAMP(timezone=True), nullable=False),
    )
    for name in (
        'fundamental_income_statement',
        'fundamental_balance_sheet',
        'fundamental_cash_flow',
    ):
        op.create_table(
            name,
            sa.Column('ticker', sa.Text(), primary_key=True),
            sa.Column('fiscal_date_ending', sa.Text(), primary_key=True),
            sa.Column('period', sa.Text(), primary_key=True),
            sa.Column('data', sa.Text()),
            sa.Column('pulled_at', sa.TIMESTAMP(timezone=True), nullable=False),
        )
    op.create_table(
        'update_log',
        sa.Column('run_time', sa.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column('ticker', sa.Text(), primary_key=True),
        sa.Column('table_name', sa.Text(), primary_key=True),
    )

def downgrade():
    for name in (
        'update_log',
        'fundamental_cash_flow',
        'fundamental_balance_sheet',
        'fundamental_income_statement',
        'fundamental_overview',
    ):
        op.drop_table(name)
