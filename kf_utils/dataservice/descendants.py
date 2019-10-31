"""
Methods for finding descendant entities (participants in families, biospecimens
in those participants, etc).
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

from kf_utils.dataservice.patch import hide_kfids, unhide_kfids
from kf_utils.dataservice.scrape import yield_entities, yield_kfids


def _accumulate(func, *args, **kwargs):
    return list(func(*args, **kwargs))


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
    bs_kfids = set(bs_kfids)
    if not gf_kfids:
        gf_kfids = set()
        with ThreadPoolExecutor() as tpex:
            futures = [
                tpex.submit(_accumulate, yield_entities, host, "biospecimen-genomic-files", {"biospecimen_id": k}, show_progress=True)
                for k in bs_kfids
            ]
            for f in as_completed(futures):
                for bg in f.result():
                    gf_kfids.add(bg["_links"]["genomic_file"].rsplit("/", 1)[1])
    else:
        gf_kfids = set(gf_kfids)

    has_extra_contributors = {
        "mixed_visibility": set(),
        "hidden": set(),
        "visible": set(),
    }
    with ThreadPoolExecutor() as tpex:
        futures = {
            tpex.submit(_accumulate, yield_entities, host, "biospecimens", {"genomic_file_id": g}, show_progress=True): g
            for g in gf_kfids
        }
        for f in as_completed(futures):
            g = futures[f]
            contribs = {
                bs["kf_id"]: (bs["visible"] is True)  # just in case any are null?
                for bs in f.result()
            }
            contrib_kfids = set(contribs.keys())
            if not contrib_kfids.issubset(bs_kfids):
                extra_kfids = contrib_kfids - bs_kfids
                extras_visible = set(contribs[k] for k in extra_kfids)
                if (False in extras_visible) and (True in extras_visible):
                    has_extra_contributors["mixed_visibility"].add(g)
                elif False in extras_visible:
                    has_extra_contributors["hidden"].add(g)
                else:
                    has_extra_contributors["visible"].add(g)
    return has_extra_contributors


def find_descendants_by_kfids(
    host, start_endpoint, start_kfids, ignore_gfs_with_hidden_external_contribs,
    kfids_only=True
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
    :param start_kfids: iterable starting kfids associated with the start_endpoint
    :param ignore_gfs_with_hidden_external_contribs: whether to ignore
        genomic files (and their descendants) that contain information from
        hidden biospecimens unrelated to the given start_kfids.
    :param kfids_only: only return KFIDs, not entire entities
    :returns: dict mapping endpoints to their sets of discovered kfids
    """

    def field(which):
        return lambda e: e[which]

    def link(which):
        return lambda e: e["_links"][which].rsplit("/", 1)[1]

    # Map of direct foreign key descendancy from families down to genomic files
    descendancy = {
        "studies": [("families", None, "study_id", field("kf_id"))],
        "families": [("participants", None, "family_id", field("kf_id"))],
        "participants": [
            ("family-relationships", None, "participant1_id", field("kf_id")),
            ("family-relationships", None, "participant2_id", field("kf_id")),
            ("outcomes", None, "participant_id", field("kf_id")),
            ("phenotypes", None, "participant_id", field("kf_id")),
            ("diagnoses", None, "participant_id", field("kf_id")),
            ("biospecimens", None, "participant_id", field("kf_id")),
        ],
        "biospecimens": [
            {
                False: ("genomic-files", None, "biospecimen_id", field("kf_id")),
                True: ("genomic-files", "biospecimen-genomic-files", "biospecimen_id", link("genomic_file"))
            },
            ("biospecimen-diagnoses", None, "biospecimen_id", field("kf_id"))
        ],
        "genomic-files": [
            ("read-groups", None, "genomic_file_id", field("kf_id")),
            ("read-group-genomic-files", None, "genomic_file_id", field("kf_id")),
            ("sequencing-experiments", None, "genomic_file_id", field("kf_id")),
            ("sequencing-experiment-genomic-files", None, "genomic_file_id", field("kf_id")),
            ("biospecimen-genomic-files", None, "genomic_file_id", field("kf_id"))
        ],
    }

    def _inner(
        host,
        endpoint,
        kfids,
        ignore_gfs_with_hidden_external_contribs,
        descendants,
    ):
        for hookup in descendancy.get(endpoint, []):
            if isinstance(hookup, dict):
                hookup = hookup[kfids_only]
            (child_endpoint, via_endpoint, foreign_key, how) = hookup
            if via_endpoint is None:
                via_endpoint = child_endpoint
            if kfids_only:
                descendants[child_endpoint] = set()
            else:
                descendants[child_endpoint] = dict()
            with ThreadPoolExecutor() as tpex:
                futures = [
                    tpex.submit(_accumulate, yield_entities, host, via_endpoint, {foreign_key: k}, show_progress=True)
                    for k in kfids
                ]
                for f in as_completed(futures):
                    for e in f.result():
                        if kfids_only:
                            descendants[child_endpoint].add(how(e))
                        else:
                            descendants[child_endpoint][how(e)] = e
            if (
                (child_endpoint == "genomic-files")
                and ignore_gfs_with_hidden_external_contribs
            ):
                # Ignore multi-specimen genomic files that have hidden
                # contributing specimens which are not in the descendants
                extra_contrib_gfs = find_gfs_with_extra_contributors(
                    host, descendants["biospecimens"],
                    descendants["genomic-files"]
                )
                to_remove = extra_contrib_gfs["hidden"] | extra_contrib_gfs["mixed_visibility"]
                if kfids_only:
                    descendants["genomic-files"] -= to_remove
                else:
                    descendants["genomic-files"] = {
                        k: v for k, v in descendants["genomic-files"].items()
                        if k not in to_remove
                    }
        for hookup in descendancy.get(endpoint, []):
            if isinstance(hookup, dict):
                hookup = hookup[kfids_only]
            (child_endpoint, _, _, _) = hookup
            _inner(
                host,
                child_endpoint,
                descendants[child_endpoint],
                ignore_gfs_with_hidden_external_contribs,
                descendants,
            )

    descendants = {start_endpoint: start_kfids}
    _inner(
        host,
        start_endpoint,
        start_kfids,
        ignore_gfs_with_hidden_external_contribs,
        descendants,
    )
    return descendants


def find_descendants_by_filter(
    host, endpoint, filter, ignore_gfs_with_hidden_external_contribs,
    kfids_only=True
):
    """
    Like find_descendants_by_kfids but starts with an endpoint filter instead
    of a list of endpoint KFIDs.
    """
    things = {
        e["kf_id"]: e
        for e in yield_entities(host, endpoint, filter, show_progress=True)
    }
    if kfids_only:
        things = set(things.keys())
    descendants = find_descendants_by_kfids(
        host, endpoint, things, ignore_gfs_with_hidden_external_contribs,
        kfids_only
    )
    return descendants


def hide_descendants_by_filter(host, endpoint, filter, gf_acl=None):
    """
    Be aware that this and unhide_descendants_by_filter are not symmetrical.

    Hiding hides partially contributed descendants, but showing only shows
    partially contributed descendants if all other contributors are visible.
    If you anticipate needing symmetrical behavior, keep a record of what you
    change.
    """
    desc = find_descendants_by_filter(host, endpoint, filter, False)
    for k, v in desc.items():
        hide_kfids(host, v, gf_acl)


def unhide_descendants_by_filter(host, endpoint, filter):
    """
    Be aware that this and hide_descendants_by_filter are not symmetrical.

    Hiding hides partially contributed descendants, but showing only shows
    partially contributed descendants if all other contributors are visible.
    If you anticipate needing symmetrical behavior, keep a record of what you
    change.
    """
    desc = find_descendants_by_filter(host, endpoint, filter, True)
    for k, v in desc.items():
        unhide_kfids(host, v)


def hide_descendants_by_kfids(host, endpoint, kfids, gf_acl=None):
    """
    Be aware that this and unhide_descendants_by_kfids are not symmetrical.

    Hiding hides partially contributed descendants, but showing only shows
    partially contributed descendants if all other contributors are visible.
    If you anticipate needing symmetrical behavior, keep a record of what you
    change.
    """
    desc = find_descendants_by_kfids(host, endpoint, kfids, False)
    for k, v in desc.items():
        hide_kfids(host, v, gf_acl)


def unhide_descendants_by_kfids(host, endpoint, kfids):
    """
    Be aware that this and hide_descendants_by_kfids are not symmetrical.

    Hiding hides partially contributed descendants, but showing only shows
    partially contributed descendants if all other contributors are visible.
    If you anticipate needing symmetrical behavior, keep a record of what you
    change.
    """
    desc = find_descendants_by_kfids(host, endpoint, kfids, True)
    for k, v in desc.items():
        unhide_kfids(host, v)
