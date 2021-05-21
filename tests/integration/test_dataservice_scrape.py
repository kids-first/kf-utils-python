import requests
from kf_utils.dataservice.scrape import (
    yield_entities,
    yield_entities_from_filter,
    yield_entities_from_kfids,
)
from tests.conftest import DATASERVICE_URL


def create_sequencing_center():
    requests.post(
        f"{DATASERVICE_URL}/sequencing-centers",
        json={"kf_id": "SC_11111111", "external_id": "x", "name": "x"},
    )


def create_study(si):
    requests.post(
        f"{DATASERVICE_URL}/studies",
        json={"kf_id": f"SD_{si}1111111", "external_id": f"{si}"},
    )


def create_participant(si, pi):
    requests.post(
        f"{DATASERVICE_URL}/participants",
        json={"kf_id": f"PT_{si}{pi}111111", "study_id": f"SD_{si}1111111", "external_id": f"{pi}"},
    )


def create_biospecimen(si, pi, bi):
    requests.post(
        f"{DATASERVICE_URL}/biospecimens",
        json={
            "kf_id": f"BS_{si}{pi}{bi}11111",
            "participant_id": f"PT_{si}{pi}111111",
            "external_sample_id": f"{pi}{bi}",
            "external_aliquot_id": f"{pi}{bi}",
            "sequencing_center_id": "SC_11111111",
            "analyte_type": "DNA",
        },
    )


def populate_data(n):
    # Create some data in dataservice
    create_sequencing_center()
    for si in range(n):
        create_study(si)
        for pi in range(n):
            create_participant(si, pi)
            for bi in range(n):
                create_biospecimen(si, pi, bi)


def test_yield_entities_from_filter(dataservice_setup):
    n = 4
    populate_data(n)

    si = 1
    filter = {"study_id": f"SD_{si}1111111"}

    # Get all participants from one study
    endpoint = "participants"
    for ps in [
        list(yield_entities_from_filter(DATASERVICE_URL, endpoint, filter)),
        list(yield_entities(DATASERVICE_URL, endpoint, filter)),
    ]:
        assert len(ps) == n
        for p in ps:
            assert p["kf_id"].startswith(f"PT_{si}")

    # Get all biospecimens from one study
    endpoint = "biospecimens"
    for bs in [
        list(yield_entities_from_filter(DATASERVICE_URL, endpoint, filter)),
        list(yield_entities(DATASERVICE_URL, endpoint, filter)),
    ]:
        assert len(bs) == (n * n)
        for b in bs:
            assert b["kf_id"].startswith(f"BS_{si}")


def test_yield_entities_from_kfids(dataservice_setup):
    n = 4
    populate_data(n)

    kfid_set = {"SD_11111111", "PT_11111111", "BS_11111111"}
    for es in [
        list(yield_entities_from_kfids(DATASERVICE_URL, kfid_set)),
        list(yield_entities(DATASERVICE_URL, None, kfid_set)),
    ]:
        assert len(es) == len(kfid_set)
        found_kfids = {e["kf_id"] for e in es}
        assert kfid_set == found_kfids
