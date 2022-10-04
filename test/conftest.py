import pytest
from dask.distributed import Client

from cosima_cookbook import database


@pytest.fixture(scope="module")
def client():
    client = Client(processes=False, dashboard_address=None)
    yield client
    client.close()


@pytest.fixture(scope="function")
def session_db(tmp_path):
    db = tmp_path / "test.db"
    s = database.create_session(str(db))
    yield s, db

    s.close()
