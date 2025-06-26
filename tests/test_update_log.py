import importlib.util
import sys
import types
from pathlib import Path

from sqlalchemy import text

root = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(root))

pkg = types.ModuleType("portfolio_analytics")
pkg.__path__ = [str(root)]

spec_core = importlib.util.spec_from_file_location(
    "portfolio_analytics.db.core", root / "db" / "core.py"
)
core = importlib.util.module_from_spec(spec_core)
spec_core.loader.exec_module(core)

spec_df = importlib.util.spec_from_file_location(
    "portfolio_analytics.classes.data_fetcher", root / "classes" / "data_fetcher.py"
)
classes_pkg = types.ModuleType("portfolio_analytics.classes")
classes_pkg.__path__ = [str(root / "classes")]
DataFetcher_mod = importlib.util.module_from_spec(spec_df)

sys.modules["portfolio_analytics"] = pkg
sys.modules["portfolio_analytics.db.core"] = core
sys.modules["portfolio_analytics.classes"] = classes_pkg
sys.modules["portfolio_analytics.classes.data_fetcher"] = DataFetcher_mod
spec_df.loader.exec_module(DataFetcher_mod)

get_engine = core.get_engine
ensure_tables = core.ensure_tables
DataFetcher = DataFetcher_mod.DataFetcher


def test_update_log_postgres(monkeypatch):
    url = "postgresql+psycopg://postgres:Secret@localhost:5432/trading_data"
    monkeypatch.setenv("DATABASE_URL", url)
    get_engine.cache_clear()
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS update_log"))

    ensure_tables()
    with engine.connect() as conn:
        exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='update_log'"
            )
        ).scalar()
    assert exists > 0

    fetcher = DataFetcher()
    fetcher._log_update("TEST", "dummy")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM update_log")).scalar()
    assert count >= 1
