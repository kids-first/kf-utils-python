import pytest
import requests
from kf_utils.dataservice.delete import delete_entities

DATASERVICE_URL = "http://localhost:5000"


@pytest.fixture
def dataservice_setup():
    """
    Delete all data in Data Service before and after tests
    """
    delete_entities(DATASERVICE_URL)
    yield
    delete_entities(DATASERVICE_URL)


def create_sequencing_center():
    kfid = "SC_11111111"
    requests.post(
        f"{DATASERVICE_URL}/sequencing-centers",
        json={"kf_id": kfid, "external_id": "x", "name": "x"},
    )
    return kfid


def create_study(si):
    kfid = f"SD_{si}1111111"
    requests.post(
        f"{DATASERVICE_URL}/studies",
        json={"kf_id": kfid, "external_id": f"{si}"},
    )
    return kfid


def create_participant(si, pi):
    kfid = f"PT_{si}{pi}111111"
    requests.post(
        f"{DATASERVICE_URL}/participants",
        json={
            "kf_id": kfid,
            "study_id": f"SD_{si}1111111",
            "external_id": f"{pi}",
        },
    )
    return kfid


def create_biospecimen(si, pi, bi):
    kfid = f"BS_{si}{pi}{bi}11111"
    requests.post(
        f"{DATASERVICE_URL}/biospecimens",
        json={
            "kf_id": kfid,
            "participant_id": f"PT_{si}{pi}111111",
            "external_sample_id": f"{pi}{bi}",
            "external_aliquot_id": f"{pi}{bi}",
            "sequencing_center_id": "SC_11111111",
            "analyte_type": "DNA",
        },
    )
    return kfid


def populate_data(n):
    # Create some data in dataservice
    kfids = []
    kfids.append(create_sequencing_center())
    for si in range(n):
        kfids.append(create_study(si))
        for pi in range(n):
            kfids.append(create_participant(si, pi))
            for bi in range(n):
                kfids.append(create_biospecimen(si, pi, bi))
    return kfids
