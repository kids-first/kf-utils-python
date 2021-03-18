import pytest
from unittest.mock import MagicMock, call

from requests.exceptions import HTTPError

from kf_utils.dataservice.delete import (
    delete_kfids,
    delete_entities,
    safe_delete,
    ENDPOINTS,
    STUDIES,
)

HOST = "http://localhost:5000"


@pytest.mark.parametrize(
    "url,safety_check,mysession,kwargs",
    [
        ("http://localhost", True, MagicMock(), {"foo": "bar"}),
        ("http://localhost", False, None, {"foo": "bar"}),
        ("http://127.0.0.1", True, MagicMock(), {"foo": "bar"}),
        ("http://127.0.0.1", False, None, {"foo": "bar"}),
        ("http://prd.dataservice.org", True, MagicMock(), {"foo": "bar"}),
        ("http://prd.dataservice.org", False, None, {"foo": "bar"}),
        ("http://10.10.1.191", True, MagicMock(), {"foo": "bar"}),
        ("http://10.10.1.191", False, None, {"foo": "bar"}),
    ]
)
def test_safe_delete(mocker, url, safety_check, mysession, kwargs):
    """
    Test kf_utils.dataservice.delete.safe_delete
    """
    # Setup mocks
    mock_session = mocker.patch(
        "kf_utils.dataservice.delete.Session"
    )()

    if safety_check and ("localhost" not in url):
        with pytest.raises(Exception) as e:
            safe_delete(
                url, safety_check=safety_check, session=session
            )
            assert "Safe delete is ENABLED" in str(e)
    else:
        safe_delete(
            url, safety_check=safety_check, session=mysession, **kwargs
        )
        if mysession:
            mock_session = mysession
        mock_session.delete.assert_called_with(url, **kwargs)


def test_delete_kfids(mocker):
    """
    Test kf_utils.dataservice.delete.delete_kfids
    """
    # Setup mocks
    mock_session = mocker.patch(
        "kf_utils.dataservice.delete.Session"
    )()
    mock_resp = MagicMock()
    mock_session.delete.return_value = mock_resp
    kfids = [f"kfid{i}" for i in range(2)]

    # Successful delete
    errors = delete_kfids(HOST, "endpoint", kfids)
    assert not errors
    assert mock_session.delete.call_count == len(kfids)
    mock_session.reset_mock()
    mock_resp.reset_mock()

    # Delete with errors
    mock_resp.raise_for_status.side_effect = HTTPError
    mock_session.delete.return_value = mock_resp
    errors = delete_kfids(HOST, "endpoint", kfids)
    assert errors
    for kfid in kfids:
        assert f"{HOST}/endpoint/{kfid}" in errors
    assert mock_session.delete.call_count == len(kfids)


def test_delete_entities(mocker):
    """
    Test kf_utils.dataservice.delete.delete_entities
    """
    mock_yield_kfids = mocker.patch(
        "kf_utils.dataservice.delete.yield_kfids"
    )
    mock_delete_kfids = mocker.patch(
        "kf_utils.dataservice.delete.delete_kfids"
    )
    kfids = [f"kfid{i}" for i in range(2)]
    study_ids = [f"study{i}" for i in range(2)]
    mock_yield_kfids.return_value = kfids
    mock_delete_kfids.return_value = {}

    # Delete entities in multiple studies
    errors = delete_entities(HOST, study_ids=study_ids)
    assert not errors
    mock_delete_kfids.assert_has_calls(
        [call(HOST, STUDIES, [sid], safety_check=True)
         for sid in study_ids],
        any_order=True
    )
    mock_yield_kfids.assert_has_calls(
        [call(HOST, e, {"study_id": sid})
         for e in ENDPOINTS for sid in study_ids],
        any_order=True
    )
    mock_yield_kfids.reset_mock()
    mock_delete_kfids.reset_mock()

    # Delete all entities
    mock_yield_kfids.return_value = study_ids
    errors = delete_entities(HOST, study_ids=None)
    assert not errors
    mock_yield_kfids.assert_has_calls(
        [call(HOST, STUDIES, {})] +
        [call(HOST, e, {"study_id": sid})
         for e in ENDPOINTS for sid in study_ids],
        any_order=True
    )
    mock_yield_kfids.reset_mock()
    mock_delete_kfids.reset_mock()

    # Delete all entities with error
    mock_errors = {"{HOST}/endpoint/{kfid}": "fake resp" for kfid in kfids}
    mock_delete_kfids.return_value = mock_errors
    errors = delete_entities(HOST, study_ids=study_ids)
    assert errors == mock_errors
