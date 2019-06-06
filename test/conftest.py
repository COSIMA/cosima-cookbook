import pytest
from dask.distributed import Client

@pytest.fixture(scope='module')
def client():
    client = Client(processes=False, dashboard_address=None)
    yield client
    client.close()
