from kf_utils.dataservice.scrape import (
    yield_entities_from_filter,
    yield_entities_from_kfids,
)
from kf_utils.dataservice.patch import (
    hide_kfids,
    unhide_kfids,
    hide_entities,
    unhide_entities,
)
from tests.conftest import DATASERVICE_URL, populate_data


def test_hide_unhide_kfids(dataservice_setup):
    n = 2
    kfids = populate_data(n)
    to_hide = {"PT_00111111", "BS_11111111"}

    hide_kfids(DATASERVICE_URL, to_hide)
    for e in yield_entities_from_kfids(DATASERVICE_URL, kfids):
        assert e["visible"] ^ (e["kf_id"] in to_hide)

    unhide_kfids(DATASERVICE_URL, to_hide)
    for e in yield_entities_from_kfids(DATASERVICE_URL, kfids):
        assert e["visible"]


def test_hide_unhide_entities(dataservice_setup):
    n = 2
    kfids = populate_data(n)
    to_hide = {"PT_00111111", "BS_11111111"}

    hide_kfids(DATASERVICE_URL, to_hide)
    entities = list(yield_entities_from_kfids(DATASERVICE_URL, kfids))
    hidden = [e for e in entities if e["kf_id"] in to_hide]

    assert not hide_entities(DATASERVICE_URL, hidden)
    entities = list(yield_entities_from_kfids(DATASERVICE_URL, kfids))
    hidden = [e for e in entities if e["kf_id"] in to_hide]
    for e in entities:
        assert e["visible"] ^ (e["kf_id"] in to_hide)

    assert unhide_entities(DATASERVICE_URL, hidden)
    entities = list(yield_entities_from_kfids(DATASERVICE_URL, kfids))
    hidden = [e for e in entities if e["kf_id"] in to_hide]
    for e in entities:
        assert e["visible"]
    
    assert not unhide_entities(DATASERVICE_URL, hidden)
    entities = list(yield_entities_from_kfids(DATASERVICE_URL, kfids))
    hidden = [e for e in entities if e["kf_id"] in to_hide]
    for e in entities:
        assert e["visible"]
