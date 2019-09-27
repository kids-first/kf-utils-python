"""
Methods for finding descendant entities (participants in families, biospecimens
in those participants, etc).
"""
from kf_utils.dataservice.scrape import yield_entities, yield_kfids


def find_genomic_files_with_extra_contributors(host, bs_kfids, gf_kfids=None):
    """
    Given a set of biospecimen KFIDs, find the KFIDs of descendant genomic
    files that also descend from biospecimes that aren't included in the given
    set. If you already know the set of descendant genomic files, you may pass
    them in to save some time.

    e.g. If BS_1234567 and BS_7654321 both contribute to GF_11112222, but you
    only specify one of the two BSIDs, then GF_11112222 will be returned.
    If you specify both of them, then GF_11112222 will _not_ be returned.

    :param host: dataservice_api_host
    ("https://kf-api-dataservice.kidsfirstdrc.org")
    :param bs_kfids: iterable of biospecimen KFIDs
    :param gf_kfids: iterable of genomic file KFIDs (optional)
    :returns: set of genomic file KFIDs satisfying the above definition
    """
    if not gf_kfids:
        gens = set()
        for k in bs_kfids:
            gens.update(
                set(yield_kfids(host, "genomic-files", {"biospecimen_id": k}, show_progress=True))
            )
    else:
        gens = set(gf_kfids)

    has_extra_contributors = set()
    for g in gens:
        contributors = set(yield_kfids(
            host, "biospecimens", {"genomic_file_id": g}, show_progress=True
        ))
        if not contributors.issubset(bs_kfids):
            has_extra_contributors.add(g)
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
    # Map of direct foreign key descendancy from families down to genomic files
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
        "biospecimens": [
            ("genomic-files", "biospecimen_id"),
            ("biospecimen-diagnoses", "biospecimen_id")
        ],
        "genomic-files": [
            ("read-groups", "genomic_file_id"),
            ("read-group-genomic-files", "genomic_file_id"),
            ("sequencing-experiments", "genomic_file_id"),
            ("sequencing-experiment-genomic-files", "genomic_file_id"),
            ("biospecimen-genomic-files", "genomic_file_id")
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
                    set(yield_kfids(host, child_endpoint, {foreign_key: k}, show_progress=True))
                )
            if (
                child_endpoint == "genomic-files"
            ) and not include_gfs_with_external_contributors:
                # ignore multi-specimen genomic files with contributors not in
                # the specified set
                descendant_kfids[
                    "genomic-files"
                ] -= find_genomic_files_with_extra_contributors(
                    host, descendant_kfids["biospecimens"],
                    descendant_kfids["genomic-files"]
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
    endpoint_kfids = set(yield_kfids(host, endpoint, filter, show_progress=True))
    kfid_sets = find_descendants_by_kfids(
        host, endpoint, endpoint_kfids, include_gfs_with_external_contributors
    )
    return kfid_sets
