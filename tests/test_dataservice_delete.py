import pytest
from unittest.mock import MagicMock, call

from requests.exceptions import HTTPError

from kf_utils.dataservice.delete import (
    delete_kfids,
    delete_entities,
    ENDPOINTS,
    STUDIES,
)

HOST = "http://localhost:5000"


@pytest.mark.parametrize(
    "url,safety_check,should_error",
    [
        ("http://localhost", True, False),
        ("http://localhost", False, False),
        ("http://localhost:5555", True, False),
        ("http://localhost:5555", False, False),
        ("http://127.0.0.1", True, False),
        ("http://127.0.0.1", False, False),
        ("http://127.0.0.1:5555", True, False),
        ("http://127.0.0.1:5555", False, False),
        ("http://prd.dataservice.org", True, True),
        ("http://prd.dataservice.org", False, False),
        ("http://prd.dataservice.org:5555", True, True),
        ("http://prd.dataservice.org:5555", False, False),
        ("http://10.10.1.191", True, True),
        ("http://10.10.1.191", False, False),
        ("http://10.10.1.191:5555", True, True),
        ("http://10.10.1.191:5555", False, False),
    ],
)
def test_safety_check(mocker, url, safety_check, should_error):
    """
    Test kf_utils.dataservice.delete.safe_delete
    """
    # Setup mocks
    mock_session = mocker.patch("kf_utils.dataservice.delete.Session")()
    mock_resp = MagicMock()
    mock_session.delete.return_value = mock_resp
    kfids = [f"PT_{i}" for i in range(2)]

    if should_error:
        with pytest.raises(Exception) as e:
            delete_kfids(url, kfids, safety_check=safety_check)
            assert "safety_check is ENABLED" in str(e)
    else:
        delete_kfids(url, kfids, safety_check=safety_check)


def test_delete_kfids(mocker):
    """
    Test kf_utils.dataservice.delete.delete_kfids
    """
    # Setup mocks
    mock_session = mocker.patch("kf_utils.dataservice.delete.Session")()
    mock_resp = MagicMock()
    mock_session.delete.return_value = mock_resp
    kfids = [f"PT_{i}" for i in range(2)]

    # Successful delete
    delete_kfids(HOST, kfids)
    assert mock_session.delete.call_count == len(kfids)
    mock_session.reset_mock()
    mock_resp.reset_mock()


def test_delete_entities(mocker):
    """
    Test kf_utils.dataservice.delete.delete_entities
    """
    mock_yield_kfids = mocker.patch("kf_utils.dataservice.delete.yield_kfids")
    mock_delete_kfids = mocker.patch("kf_utils.dataservice.delete.delete_kfids")
    kfids = [f"PT_{i}" for i in range(2)]
    study_ids = [f"SD_{i}" for i in range(2)]
    mock_yield_kfids.return_value = kfids
    mock_delete_kfids.return_value = {}

    # Delete entities in multiple studies
    delete_entities(HOST, study_ids=study_ids)
    mock_delete_kfids.assert_has_calls(
        [call(HOST, [sid], safety_check=True) for sid in study_ids],
        any_order=True,
    )
    mock_yield_kfids.assert_has_calls(
        [
            call(HOST, e, {"study_id": sid}, show_progress=True)
            for e in ENDPOINTS
            for sid in study_ids
        ],
        any_order=True,
    )
    mock_yield_kfids.reset_mock()
    mock_delete_kfids.reset_mock()

    # Delete all entities
    mock_yield_kfids.return_value = study_ids
    delete_entities(HOST, study_ids=None)
    mock_yield_kfids.assert_has_calls(
        [call(HOST, STUDIES, {}, show_progress=True)]
        + [
            call(HOST, e, {}, show_progress=True)
            for e in ENDPOINTS
        ],
        any_order=True,
    )
    mock_yield_kfids.reset_mock()
    mock_delete_kfids.reset_mock()
