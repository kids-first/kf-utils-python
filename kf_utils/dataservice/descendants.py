"""
Methods for finding descendant entities (participants in families, biospecimens
in those participants, etc).
"""
from kf_utils.dataservice.patch import hide_kfids, show_kfids
from kf_utils.dataservice.scrape import yield_entities, yield_kfids


def find_gfs_with_extra_contributors(host, bs_kfids, gf_kfids=None):
    """
    Given a set of biospecimen KFIDs, find the KFIDs of descendant genomic
    files that also descend from biospecimens that aren't included in the given
    set. If you already know the full set of descendant genomic files, you may
    pass them in to save some time.

    :param host: dataservice_api_host
        e.g. "https://kf-api-dataservice.kidsfirstdrc.org"
    :param bs_kfids: iterable of biospecimen KFIDs
    :param gf_kfids: iterable of genomic file KFIDs (optional)
    :returns: sets of KFIDs of genomic files with contributing biospecimens not
        included in bs_kfids, divided into these groups:
            "all_visible": all extra contributors are visible in the dataservice
            "all_hidden": all extra contributors are hidden in the dataservice
            "mixed_visibility": some extra contributors are hidden and some not

    Example:
    If BS_12345678 and BS_87654321 both contribute to GF_11112222, but you only
    specify one of the two BSIDs, then GF_11112222 will be returned. If you
    specify both of them, then GF_11112222 will _not_ be returned. The exact
    nature of the return will depend on the visibility of the extra
    contributors.
    """
    if not gf_kfids:
        gens = set()
        for k in bs_kfids:
            gens.update(
                set(yield_kfids(host, "genomic-files", {"biospecimen_id": k}, show_progress=True))
            )
    else:
        gens = set(gf_kfids)

    has_extra_contributors = {
        "mixed_visibility": set(),
        "hidden": set(),
        "visible": set(),
    }
    for g in gens:
        contribs = {
            e["kf_id"]: (e["visible"] == True)  # just in case any are null?
            for e in yield_entities(
                host, "biospecimens", {"genomic_file_id": g}, show_progress=True
            )
        }
        contrib_kfids = set(contribs.keys())
        bs_kfids = set(bs_kfids)
        if not contrib_kfids.issubset(bs_kfids):
            extra_kfids = contrib_kfids - bs_kfids
            extras_visible = set(contribs[k] for k in extra_kfids)
            if (False in extras_visible) and (True in extras_visible):
                has_extra_contributors["mixed_visibility"].add(g)
            elif False in extras_visible:
                has_extra_contributors["all_hidden"].add(g)
            else:
                has_extra_contributors["all_visible"].add(g)
    return has_extra_contributors


def find_descendants_by_kfids(
    host, start_endpoint, start_kfids, ignore_gfs_with_hidden_external_contribs
):
    """
    Given a set of KFIDs from a specified endpoint, find the KFIDs of all
    descendant entities.

    Given a family kfid, the result will be all participants in that family,
    all of the participants' biospecimens/outcomes/phenotypes/etc, all of
    their biospecimens' resultant genomic files, and all of the genomic files'
    sequencing experiments and read groups.

    Given a set of genomic file kfids, the result will be just their sequencing
    experiments and read groups.

    If you plan to make the discovered descendants visible, you should set
    ignore_gfs_with_hidden_external_contribs=True so that you don't accidentally
    unhide a genomic file that has hidden contributing biospecimens.

    If you plan to make the discovered descendants hidden, you should set
    ignore_gfs_with_hidden_external_contribs=False so that everything linked to
    the hidden biospecimens also get hidden.

    :param host: dataservice_api_host ("https://kf-api-dataservice.kidsfirstdrc.org")
    :param start_endpoint: endpoint of the starting kfids being passed in
    :param start_kfids: starting kfids associated with the start_endpoint
    :param ignore_gfs_with_hidden_external_contribs: whether to ignore
        genomic files (and their descendants) that contain information from
        hidden biospecimens unrelated to the given start_kfids.
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
        ignore_gfs_with_hidden_external_contribs,
        descendant_kfids,
    ):
        for (child_endpoint, foreign_key) in descendancy.get(endpoint, []):
            descendant_kfids[child_endpoint] = set()
            for k in kfids:
                descendant_kfids[child_endpoint].update(
                    set(yield_kfids(host, child_endpoint, {foreign_key: k}, show_progress=True))
                )
            if (
                (child_endpoint == "genomic-files")
                and ignore_gfs_with_hidden_external_contribs
            ):
                # Ignore multi-specimen genomic files that have hidden
                # contributing specimens which are not in the descendants
                extra_contrib_gfs = find_gfs_with_extra_contributors(
                    host, descendant_kfids["biospecimens"],
                    descendant_kfids["genomic-files"]
                )
                descendant_kfids["genomic-files"] -= extra_contrib_gfs["all_hidden"]
                descendant_kfids["genomic-files"] -= extra_contrib_gfs["mixed_visibility"]
            _inner(
                host,
                child_endpoint,
                descendant_kfids[child_endpoint],
                ignore_gfs_with_hidden_external_contribs,
                descendant_kfids,
            )

    descendant_kfids = {start_endpoint: start_kfids}
    _inner(
        host,
        start_endpoint,
        start_kfids,
        ignore_gfs_with_hidden_external_contribs,
        descendant_kfids,
    )
    return descendant_kfids


def find_descendants_by_filter(
    host, endpoint, filter, ignore_gfs_with_hidden_external_contribs
):
    """
    Like find_descendants_by_kfids but starts with an endpoint filter instead
    of a list of endpoint KFIDs.
    """
    endpoint_kfids = set(yield_kfids(host, endpoint, filter, show_progress=True))
    kfid_sets = find_descendants_by_kfids(
        host, endpoint, endpoint_kfids, ignore_gfs_with_hidden_external_contribs
    )
    return kfid_sets


def hide_all_descendants_by_filter(host, endpoint, filter):
    """
    Be aware that this and show_all_descendants_by_filter are not symmetrical.

    Hiding hides partially contributed descendants, but showing only shows
    partially contributed descendants if all other contributors are visible.
    If you anticipate needing symmetrical behavior, keep a record of what you
    change.
    """
    desc = find_descendants_by_filter(host, endpoint, filter, False)
    for k, v in desc.items():
        hide_kfids(host, v)


def show_all_descendants_by_filter(host, endpoint, filter):
    """
    Be aware that this and hide_all_descendants_by_filter are not symmetrical.

    Hiding hides partially contributed descendants, but showing only shows
    partially contributed descendants if all other contributors are visible.
    If you anticipate needing symmetrical behavior, keep a record of what you
    change.
    """
    desc = find_descendants_by_filter(host, endpoint, filter, True)
    for k, v in desc.items():
        show_kfids(host, v)
