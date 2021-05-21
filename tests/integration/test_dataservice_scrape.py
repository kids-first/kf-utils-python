from kf_utils.dataservice.scrape import (
    yield_entities,
    yield_entities_from_filter,
    yield_entities_from_kfids,
)
from tests.conftest import DATASERVICE_URL, populate_data


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
