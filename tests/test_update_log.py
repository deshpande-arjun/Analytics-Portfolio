import importlib.util
import types
from pathlib import Path
from sqlalchemy import text

root = Path(__file__).resolve().parents[1]

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

sys_modules_backup = dict(core=None)
# register modules
import sys
sys.modules["portfolio_analytics"] = pkg
sys.modules["portfolio_analytics.db.core"] = core
sys.modules["portfolio_analytics.classes"] = classes_pkg
sys.modules["portfolio_analytics.classes.data_fetcher"] = DataFetcher_mod
spec_df.loader.exec_module(DataFetcher_mod)

get_engine = core.get_engine
ensure_tables = core.ensure_tables
DataFetcher = DataFetcher_mod.DataFetcher


def test_log_update(tmp_path, monkeypatch):
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_file}")
    get_engine.cache_clear()
    ensure_tables()
    fetcher = DataFetcher()
    fetcher._log_update("TEST", "example")
    engine = get_engine()
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM update_log")).scalar()
    assert count > 0
