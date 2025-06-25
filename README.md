# Portfolio Analytics


Analytics-Portfolio folder has:
classes
    market_data.py
    data_fetcher.py        - unified helper for Alpha Vantage fundamentals, price data,
                             technical indicators, economic data and news sentiment.
    alpha_vantage_data.py  - thin wrapper aliasing ``DataFetcher`` for backwards
                             compatibility.
    portfolio_calculations.py
    portfolio_decomposer.py
    database_accessor.py - read stored Alpha Vantage data and join price/fundamental tables
scripts
    main_script.py
    rough_script.py
__init__.py
setup.py
config.py
logging_utils.py - helper to configure rotating loggers
date_utils.py    - pandas-friendly date utilities

## Database configuration

Set the `DATABASE_URL` environment variable (see `.env.example`) to point at
your PostgreSQL or other SQLAlchemy-supported database. The codebase no longer
uses `sqlite3` directly and now relies on SQLAlchemy for connection pooling and
database-agnostic queries.
