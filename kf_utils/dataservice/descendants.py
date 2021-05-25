"""
Methods for finding descendant entities (participants in families, biospecimens
in those participants, etc).
"""
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import psycopg2.extras
from kf_utils.dataservice.patch import hide_entities, unhide_entities
from kf_utils.dataservice.scrape import yield_entities


def _accumulate(func, *args, **kwargs):
    return list(func(*args, **kwargs))


# Maps of direct foreign key descendancy from studies down to genomic files
# {parent_endpoint: [(child_endpoint, link_on_parent, link_on_child), ...], ...}
_db_descendancy = {
    "study": [
        ("participant", "kf_id", "study_id"),
        # We need to specially handle getting to families from studies, because
        # the database layout does not match the logical data arrangement, so
        # just add a stub here for family.
        ("family", None, None),
    ],
    "family": [("participant", "kf_id", "family_id")],
    "participant": [
        ("family_relationship", "kf_id", "participant1_id"),
        ("family_relationship", "kf_id", "participant2_id"),
        ("outcome", "kf_id", "participant_id"),
        ("phenotype", "kf_id", "participant_id"),
        ("diagnosis", "kf_id", "participant_id"),
        ("biospecimen", "kf_id", "participant_id"),
    ],
    "biospecimen": [
        ("biospecimen_genomic_file", "kf_id", "biospecimen_id"),
        ("biospecimen_diagnosis", "kf_id", "biospecimen_id"),
    ],
    "biospecimen_genomic_file": [("genomic_file", "genomic_file_id", "kf_id")],
    "genomic_file": [
        ("read_group_genomic_file", "kf_id", "genomic_file_id"),
        ("sequencing_experiment_genomic_file", "kf_id", "genomic_file_id"),
        ("biospecimen_genomic_file", "kf_id", "genomic_file_id"),
    ],
    "read_group_genomic_file": [("read_group", "read_group_id", "kf_id")],
    "sequencing_experiment_genomic_file": [
        ("sequencing_experiment", "sequencing_experiment_id", "kf_id")
    ],
}
_api_descendancy = {
    "studies": [
        ("participants", "kf_id", "study_id"),
        ("families", "kf_id", "study_id"),
    ],
    "families": [("participants", "kf_id", "family_id")],
    "participants": [
        ("family-relationships", "kf_id", "participant1_id"),
        ("family-relationships", "kf_id", "participant2_id"),
        ("outcomes", "kf_id", "participant_id"),
        ("phenotypes", "kf_id", "participant_id"),
        ("diagnoses", "kf_id", "participant_id"),
        ("biospecimens", "kf_id", "participant_id"),
    ],
    "biospecimens": [
        ("genomic-files", "kf_id", "biospecimen_id"),
        ("biospecimen-diagnoses", "kf_id", "biospecimen_id"),
    ],
    "genomic-files": [
        ("read-groups", "kf_id", "genomic_file_id"),
        ("read-group-genomic-files", "kf_id", "genomic_file_id"),
        ("sequencing-experiments", "kf_id", "genomic_file_id"),
        ("sequencing-experiment-genomic-files", "kf_id", "genomic_file_id"),
        ("biospecimen-genomic-files", "kf_id", "genomic_file_id"),
    ],
}


def find_gfs_with_extra_contributors(api_or_db_url, bs_kfids, gf_kfids=None):
    """
    Given a set of biospecimen KFIDs, find the KFIDs of descendant genomic
    files that also descend from biospecimens that aren't included in the given
    set. If you already know the full set of descendant genomic files, you may
    pass them in to save some time.

    Special performance note: a database connect url will run MUCH faster
    compared to a dataservice api host

    :param api_or_db_url: dataservice api host _or_ database connect url
        e.g. "https://kf-api-dataservice.kidsfirstdrc.org" or
        "postgres://<USERNAME>:<PASSWORD>@kf-dataservice-postgres-prd.kids-first.io:5432/kfpostgresprd"
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
    if api_or_db_url.startswith(("http:", "https:")):
        return _find_gfs_with_extra_contributors_with_http_api(
            api_or_db_url, bs_kfids, gf_kfids=None
        )
    else:
        return _find_gfs_with_extra_contributors_with_db_conn(
            api_or_db_url, bs_kfids, gf_kfids=None
        )


def _find_gfs_with_extra_contributors_with_db_conn(
    db_url, bs_kfids, gf_kfids=None
):
    """See find_gfs_with_extra_contributors"""
    sql = (
        "select distinct extra.genomic_file_id, biospecimen.visible from"
        " biospecimen_genomic_file bg join biospecimen_genomic_file extra"
        " on bg.genomic_file_id = extra.genomic_file_id"
        " join biospecimen"
        " on biospecimen.kf_id = extra.biospecimen_id"
        " where bg.biospecimen_id in %s and extra.biospecimen_id not in %s"
    )

    bs_kfids = tuple(bs_kfids)
    kfid_tuples = (bs_kfids, bs_kfids)
    if gf_kfids:
        kfid_tuples = (bs_kfids, bs_kfids, tuple(gf_kfids))
        sql += " and extra.genomic_file_id in %s"

    has_extra_contributors = {
        "mixed_visibility": set(),
        "hidden": set(),
        "visible": set(),
    }

    storage = defaultdict(set)
    with psycopg2.connect(db_url) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql, kfid_tuples)
            for r in cur.fetchall():
                storage[r["genomic_file_id"]].add(r["visible"])

    for gfid, visset in storage.items():
        if (False in visset) and (True in visset):
            has_extra_contributors["mixed_visibility"].add(gfid)
        elif False in visset:
            has_extra_contributors["hidden"].add(gfid)
        else:
            has_extra_contributors["visible"].add(gfid)

    return has_extra_contributors


def _find_gfs_with_extra_contributors_with_http_api(
    api_url, bs_kfids, gf_kfids=None
):
    """See find_gfs_with_extra_contributors"""
    bs_kfids = set(bs_kfids)
    if not gf_kfids:
        gf_kfids = set()
        with ThreadPoolExecutor() as tpex:
            futures = [
                tpex.submit(
                    _accumulate,
                    yield_entities,
                    api_url,
                    "biospecimen-genomic-files",
                    {"biospecimen_id": k},
                    show_progress=True,
                )
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
            tpex.submit(
                _accumulate,
                yield_entities,
                api_url,
                "biospecimens",
                {"genomic_file_id": g},
                show_progress=True,
            ): g
            for g in gf_kfids
        }
        for f in as_completed(futures):
            g = futures[f]
            contribs = {
                bs["kf_id"]: (bs["visible"] is True) for bs in f.result()
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
    api_or_db_url,
    parent_endpoint,
    parents,
    ignore_gfs_with_hidden_external_contribs,
    kfids_only=True,
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

    Special performance note: a database connect url will run MUCH faster
    compared to a dataservice api host

    :param api_or_db_url: dataservice api host _or_ database connect url
        e.g. "https://kf-api-dataservice.kidsfirstdrc.org" or
        "postgres://<USERNAME>:<PASSWORD>@kf-dataservice-postgres-prd.kids-first.io:5432/kfpostgresprd"
    :param parent_endpoint: endpoint of the starting kfids being passed in
    :param parents: iterable of starting kfids or entities associated with the
        parent_endpoint
    :param ignore_gfs_with_hidden_external_contribs: whether to ignore
        genomic files (and their descendants) that contain information from
        hidden biospecimens unrelated to the given parents.
    :param kfids_only: only return KFIDs, not entire entities
    :returns: dict mapping endpoints to their sets of discovered kfids
    """
    use_api = api_or_db_url.startswith(("http:", "https:"))

    if use_api:
        parent_type = parent_endpoint
    else:
        endpoint_to_table = {
            "studies": "study",
            "participants": "participant",
            "family-relationships": "family_relationship",
            "outcomes": "outcome",
            "phenotypes": "phenotype",
            "diagnoses": "diagnosis",
            "biospecimens": "biospecimen",
            "families": "family",
            "biospecimen-genomic-files": "biospecimen_genomic_file",
            "biospecimen-diagnoses": "biospecimen_diagnosis",
            "genomic-files": "genomic_file",
            "read-group-genomic-files": "read_group_genomic_file",
            "sequencing-experiment-genomic-files": "sequencing_experiment_genomic_file",
            "read-groups": "read_group",
            "sequencing-experiments": "sequencing_experiment",
        }
        table_to_endpoint = {v: k for k, v in endpoint_to_table.items()}
        parent_type = endpoint_to_table[parent_endpoint]

    if use_api:
        descendancy = _api_descendancy
    else:
        descendancy = _db_descendancy
        db_conn = psycopg2.connect(api_or_db_url)
        db_cur = db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if isinstance(parents, str):
        parents = [parents]

    if isinstance(next(iter(parents), None), dict):
        parent_kfids = set(p["kf_id"] for p in parents)
        descendants = {parent_type: {p["kf_id"]: p for p in parents}}
    else:
        parent_kfids = set(parents)
        if use_api:
            descendants = {
                parent_type: {
                    e["kf_id"]: e
                    for e in yield_entities(api_or_db_url, None, parent_kfids)
                }
            }
        else:
            query = f"select distinct * from {parent_type} where kf_id in %s"
            db_cur.execute(query, (tuple(parent_kfids | {None}),))
            descendants = {
                parent_type: {p["kf_id"]: dict(p) for p in db_cur.fetchall()}
            }

    done = set()
    for t in descendancy.keys():
        if t != parent_type:
            done.add(t)
        else:
            break

    def _inner(parent_type, parent_kfids, descendants):
        if parent_type in done:
            return
        done.add(parent_type)
        for (child_type, link_on_parent, link_on_child) in descendancy.get(
            parent_type, []
        ):
            if use_api:
                with ThreadPoolExecutor() as tpex:
                    futures = [
                        tpex.submit(
                            _accumulate,
                            yield_entities,
                            api_or_db_url,
                            child_type,
                            {link_on_child: k},
                            show_progress=True,
                        )
                        for k in parent_kfids
                    ]
                    children = {
                        e["kf_id"]: e
                        for f in as_completed(futures)
                        for e in f.result()
                    }
            else:
                # special case for getting to families from studies
                if parent_type == "study" and child_type == "family":
                    query = (
                        "select distinct family.* from family join participant"
                        " on participant.family_id = family.kf_id join study on"
                        " participant.study_id = study.kf_id where study.kf_id "
                        "in %s"
                    )
                else:
                    query = (
                        f"select distinct {child_type}.* from {child_type} join {parent_type}"
                        f" on {child_type}.{link_on_child} = {parent_type}.{link_on_parent}"
                        f" where {parent_type}.kf_id in %s"
                    )
                db_cur.execute(query, (tuple(parent_kfids | {None}),))
                children = {c["kf_id"]: dict(c) for c in db_cur.fetchall()}

            if children:
                descendants[child_type] = descendants.get(child_type, dict())
                descendants[child_type].update(children)

            if (
                child_type == "genomic_file"
            ) and ignore_gfs_with_hidden_external_contribs:
                # Ignore multi-specimen genomic files that have hidden
                # contributing specimens which are not in the descendants
                extra_contrib_gfs = find_gfs_with_extra_contributors(
                    api_or_db_url,
                    descendants["biospecimen"],
                    descendants["genomic_file"],
                )
                to_remove = (
                    extra_contrib_gfs["hidden"]
                    | extra_contrib_gfs["mixed_visibility"]
                )
                descendants["genomic_file"] = {
                    k: v
                    for k, v in descendants["genomic_file"].items()
                    if k not in to_remove
                }
        for (child_type, _, _) in descendancy.get(parent_type, []):
            if descendants.get(child_type):
                _inner(child_type, descendants[child_type].keys(), descendants)

    _inner(parent_type, parent_kfids, descendants)

    if not use_api:
        descendants = {table_to_endpoint[k]: v for k, v in descendants.items()}

    if kfids_only:
        for k, v in descendants.items():
            descendants[k] = set(descendants[k])

    return descendants


def find_descendants_by_filter(
    api_url,
    endpoint,
    filter,
    ignore_gfs_with_hidden_external_contribs,
    kfids_only=True,
    db_url=None,
):
    """
    Similar to find_descendants_by_kfids but starts with an API endpoint filter
    instead of a list of endpoint KFIDs.
    """
    things = list(yield_entities(api_url, endpoint, filter, show_progress=True))
    if kfids_only:
        things = [t["kf_id"] for t in things]

    descendants = find_descendants_by_kfids(
        db_url or api_url,
        endpoint,
        things,
        ignore_gfs_with_hidden_external_contribs,
        kfids_only=kfids_only,
    )
    return descendants


def hide_descendants_by_filter(
    api_url, endpoint, filter, gf_acl=None, db_url=None, dry_run=False
):
    """
    Be aware that this and unhide_descendants_by_filter are not symmetrical.

    Hiding hides partially contributed descendants, but showing only shows
    partially contributed descendants if all other contributors are visible.
    If you anticipate needing symmetrical behavior, keep a record of what you
    change.
    """
    desc = find_descendants_by_filter(
        api_url,
        endpoint,
        filter,
        ignore_gfs_with_hidden_external_contribs=False,
        kfids_only=False,
        db_url=db_url,
    )
    changed = []
    for es in desc.values():
        changed.extend(hide_entities(api_url, es.values(), gf_acl, dry_run))
    return changed


def unhide_descendants_by_filter(
    api_url, endpoint, filter, db_url=None, dry_run=False
):
    """
    Be aware that this and hide_descendants_by_filter are not symmetrical.

    Hiding hides partially contributed descendants, but showing only shows
    partially contributed descendants if all other contributors are visible.
    If you anticipate needing symmetrical behavior, keep a record of what you
    change.
    """
    desc = find_descendants_by_filter(
        api_url,
        endpoint,
        filter,
        ignore_gfs_with_hidden_external_contribs=True,
        kfids_only=False,
        db_url=db_url,
    )
    changed = []
    for es in desc.values():
        changed.extend(unhide_entities(api_url, es.values(), dry_run))
    return changed


def hide_descendants_by_kfids(
    api_url, endpoint, kfids, gf_acl=None, db_url=None, dry_run=False
):
    """
    Be aware that this and unhide_descendants_by_kfids are not symmetrical.

    Hiding hides partially contributed descendants, but showing only shows
    partially contributed descendants if all other contributors are visible.
    If you anticipate needing symmetrical behavior, keep a record of what you
    change.
    """
    desc = find_descendants_by_kfids(
        db_url or api_url,
        endpoint,
        kfids,
        ignore_gfs_with_hidden_external_contribs=False,
        kfids_only=False,
    )
    changed = []
    for es in desc.values():
        changed.extend(hide_entities(api_url, es.values(), gf_acl, dry_run))
    return changed


def unhide_descendants_by_kfids(
    api_url, endpoint, kfids, db_url=None, dry_run=False
):
    """
    Be aware that this and hide_descendants_by_kfids are not symmetrical.

    Hiding hides partially contributed descendants, but showing only shows
    partially contributed descendants if all other contributors are visible.
    If you anticipate needing symmetrical behavior, keep a record of what you
    change.
    """
    desc = find_descendants_by_kfids(
        db_url or api_url,
        endpoint,
        kfids,
        ignore_gfs_with_hidden_external_contribs=True,
        kfids_only=False,
    )
    changed = []
    for es in desc.values():
        changed.extend(unhide_entities(api_url, es.values(), dry_run))
    return changed
