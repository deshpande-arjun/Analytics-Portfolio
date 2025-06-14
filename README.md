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
