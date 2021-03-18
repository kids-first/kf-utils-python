import pytest
from kf_utils.dataservice.delete import delete_entities

DATASERVICE_URL="http://localhost:5000"

@pytest.fixture
def dataservice_setup():
    """
    Delete all data in Data Service before and after tests
    """
    delete_entities(DATASERVICE_URL)
    yield
    delete_entities(DATASERVICE_URL)

