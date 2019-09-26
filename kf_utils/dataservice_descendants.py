"""
Methods for finding descendant entities (participants in families, biospecimens
in those participants, etc).
"""
from kf_utils.dataservice_scrape import yield_entities


def __find_kfids_by_filter(host, endpoint, filter):
    """
    Wrapper for returning a set of only KFIDs from
    kf_utils.dataservice_scrape.yield_entities
    """
    kfids = set()
    for e in yield_entities(host, endpoint, filter):
        kfids.add(e["kf_id"])
        print(".", end="", flush=True)
    return kfids


def find_descendant_genomic_files_with_extra_contributors(host, specimen_kfids):
    """
    Given a set of biospecimen KFIDs, find the KFIDs of descendant genomic
    files that also descend from biospecimes that aren't included in the given
    set.

    e.g. If BS_1234567 and BS_7654321 both contribute to GF_11112222, but you
    only specify one of the two BSIDs, then GF_11112222 will be returned.
    If you specify both of them, then GF_11112222 will _not_ be returned.

    :param host: dataservice_api_host ("https://kf-api-dataservice.kidsfirstdrc.org")
    :param specimen_kfids: iterable of biospecimen KFIDs
    :returns: set of genomic file KFIDs satisfying the above definition
    """
    gens = set()
    for k in specimen_kfids:
        gens.update(
            __find_kfids_by_filter(host, "genomic-files", {"biospecimen_id": k})
        )
    has_extra_contributors = set()
    for g in gens:
        contributors = __find_kfids_by_filter(
            host, "biospecimens", {"genomic_file_id": g}
        )
        if not contributors.issubset(specimen_kfids):
            has_extra_contributors.add(g)
    endpoint = "genomic-files"
    return has_extra_contributors


def find_descendants_by_kfids(
    host, start_endpoint, start_kfids, include_gfs_with_external_contributors=False
):
    """
    Given a set of KFIDs from a specified endpoint, find the KFIDs of all
    descendant entities.

    e.g.

    Given a family kfid, the result will be all participants in that family,
    all of the participants' biospecimens/outcomes/phenotypes/etc, all of
    their biospecimens' resultant genomic files, and all of the genomic files'
    sequencing experiments and read groups.

    Given a set of genomic file kfids, the result will be just their sequencing
    experiments and read groups.

    :param host: dataservice_api_host ("https://kf-api-dataservice.kidsfirstdrc.org")
    :param start_endpoint: endpoint of the starting kfids being passed in
    :param start_kfids: starting kfids associated with the start_endpoint
    :param include_gfs_with_external_contributors: whether to also include
        genomic files (and descendants) that contain information from biospecimens
        unrelated to the given start_kfids.
    :returns: dict mapping endpoints to their sets of discovered kfids
    """
    # Storage for direct foreign key descendancy from families down to genomic
    # files, not including pure relationship tables like BSDG.
    descendancy = {
        "families": [("participants", "family_id")],
        "participants": [
            ("family-relationships", "participant1_id"),
            ("family-relationships", "participant2_id"),
            ("outcomes", "participant_id"),
            ("phenotypes", "participant_id"),
            ("diagnoses", "participant_id"),
            ("biospecimens", "participant_id"),
        ],
        "biospecimens": [("genomic-files", "biospecimen_id")],
        "genomic-files": [
            ("read-groups", "genomic_file_id"),
            ("sequencing-experiments", "genomic_file_id"),
        ],
    }

    def _inner(
        host,
        endpoint,
        kfids,
        include_gfs_with_external_contributors=False,
        descendant_kfids=None,
    ):
        for (child_endpoint, foreign_key) in descendancy.get(endpoint, []):
            descendant_kfids[child_endpoint] = set()
            for k in kfids:
                descendant_kfids[child_endpoint].update(
                    __find_kfids_by_filter(host, child_endpoint, {foreign_key: k})
                )
            if (
                child_endpoint == "genomic-files"
            ) and not include_gfs_with_external_contributors:
                # ignore multi-specimen genomic files with contributors not in
                # the specified set
                descendant_kfids[
                    "genomic-files"
                ] -= find_descendant_genomic_files_with_extra_contributors(
                    host, descendant_kfids["biospecimens"]
                )
            _inner(
                host,
                child_endpoint,
                descendant_kfids[child_endpoint],
                include_gfs_with_external_contributors,
                descendant_kfids,
            )

    descendant_kfids = {start_endpoint: start_kfids}
    _inner(
        host,
        start_endpoint,
        start_kfids,
        include_gfs_with_external_contributors,
        descendant_kfids,
    )
    return descendant_kfids


def find_descendants_by_filter(
    host, endpoint, filter, include_gfs_with_external_contributors=False
):
    """
    Like find_descendants_by_kfids but starts with an endpoint filter instead
    of a list of endpoint KFIDs.
    """
    endpoint_kfids = __find_kfids_by_filter(host, endpoint, filter)
    kfid_sets = find_descendants_by_kfids(
        host, endpoint, endpoint_kfids, include_gfs_with_external_contributors
    )
    return kfid_sets
