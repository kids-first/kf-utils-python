from kf_utils.dataservice.descendants import find_descendants_by_kfids, find_descendants_by_filter
from tests.conftest import DATASERVICE_URL, populate_data


def test_by_kfids_simple(dataservice_setup):
    n = 4
    populate_data(n)

    desc = find_descendants_by_kfids(
        DATASERVICE_URL, "participants", ["PT_11111111"],
        ignore_gfs_with_hidden_external_contribs=False,
        kfids_only=False
    )
    assert len(desc["participants"]) == 1
    assert len(desc["biospecimens"]) == n
    # parent level should be populated
    assert isinstance(desc["participants"]["PT_11111111"], dict)

    desc = find_descendants_by_kfids(
        DATASERVICE_URL, "participants", [
            "PT_10111111", "PT_11111111", "PT_12111111", "PT_13111111"
        ],
        ignore_gfs_with_hidden_external_contribs=False,
        kfids_only=False
    )
    assert len(desc["participants"]) == n
    assert len(desc["biospecimens"]) == n*n

    desc = find_descendants_by_kfids(
        DATASERVICE_URL, "studies", ["SD_11111111"],
        ignore_gfs_with_hidden_external_contribs=False,
        kfids_only=False
    )
    assert len(desc["participants"]) == n
    assert len(desc["biospecimens"]) == n*n


def test_by_filter_simple(dataservice_setup):
    n = 4
    populate_data(n)
    desc = find_descendants_by_filter(
        DATASERVICE_URL, "participants", {"study_id": "SD_11111111"},
        ignore_gfs_with_hidden_external_contribs=False,
        kfids_only=False
    )
    assert len(desc["participants"]) == n
    assert len(desc["biospecimens"]) == n*n
    # parent level should be populated
    assert isinstance(desc["participants"]["PT_11111111"], dict)
