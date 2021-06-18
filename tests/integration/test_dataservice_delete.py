from tests.conftest import DATASERVICE_URL, populate_data

import requests
from kf_utils.dataservice.delete import delete_entities, ENDPOINTS, STUDIES


def test_delete_entities(dataservice_setup):
    """
    Test kf_utils.dataservice.delete.delete_entities against live Data Service
    """
    # Create some data in dataservice
    n_studies = 4
    n_participants = 2
    data = {
        "studies": [
            {"external_id": f"study_{i}", "kf_id": f"SD_{i}1111111"}
            for i in range(n_studies)
        ],
        "participants": [
            {
                "kf_id": f"PT_{i}{j}111111",
                "gender": "Female",
                "study_id": f"SD_{i}1111111",
            }
            for i in range(n_studies)
            for j in range(n_participants)
        ],
    }
    for endpoint, payloads in data.items():
        for p in payloads:
            resp = requests.post(f"{DATASERVICE_URL}/{endpoint}", json=p)

    # Delete first two studies
    sids = [s["kf_id"] for s in data["studies"][0:2]]
    delete_entities(DATASERVICE_URL, study_ids=sids)

    # Check first two studies deleted, other studies remain
    for i in range(n_studies):
        kfid = data["studies"][i]["kf_id"]
        params = {"study_id": kfid}
        study_resp = requests.get(f"{DATASERVICE_URL}/studies/{kfid}")
        part_resp = requests.get(
            f"{DATASERVICE_URL}/participants", params=params
        )
        if i <= 1:
            assert study_resp.status_code == 404
            assert part_resp.json()["total"] == 0
        else:
            assert study_resp.status_code == 200
            assert part_resp.json()["total"] == n_participants

    # Delete all studies
    delete_entities(DATASERVICE_URL)

    # Check all study entities deleted
    for endpoint in ENDPOINTS + [STUDIES]:
        resp = requests.get(f"{DATASERVICE_URL}/{endpoint}")
        assert resp.json()["total"] == 0


def test_delete_many(dataservice_setup):
    # iterating the wrong way clogs an executor
    # see https://github.com/kids-first/kf-utils-python/pull/36#pullrequestreview-685334120
    populate_data(1, 500, 0)
    delete_entities(DATASERVICE_URL)
