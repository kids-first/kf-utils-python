from concurrent.futures import ThreadPoolExecutor
from pprint import pformat
from urllib.parse import urlparse

from d3b_utils.requests_retry import Session
from kf_utils.dataservice.meta import get_endpoint
from kf_utils.dataservice.scrape import yield_kfids

# DO NOT RE-ORDER - deletion requires this order
ENDPOINTS = [
    "read-groups",
    "read-group-genomic-files",
    "sequencing-experiments",
    "sequencing-experiment-genomic-files",
    "genomic-files",
    "biospecimen-genomic-files",
    "biospecimens",
    "outcomes",
    "phenotypes",
    "diagnoses",
    "participants",
    "family-relationships",
    "families",
    "sample",
]
STUDIES = "studies"
LOCAL_HOSTS = {
    "localhost",
    "127.0.0.1",
}


def delete_kfids(host, kfids, safety_check=True):
    """
    Rapidly delete entities by KF ID. Default behavior only deletes resources
    at localhost unless safety_check=False

    :param host: dataservice base url string (e.g. "http://localhost:5000")
    :type host: str
    :param kfids: Data Service Kids First IDs
    :type kfids: iterable of strs
    """
    kfids = list(kfids)
    host = host.strip("/")
    base = urlparse(host).netloc.split(":")[0]
    if safety_check and (base not in LOCAL_HOSTS):
        raise Exception(
            f"Cannot delete from {host} because safety_check is ENABLED. "
            f"Resources that are not in {LOCAL_HOSTS} will not be deleted "
            "unless you set safety_check=False."
        )

    def delete(u):
        return Session().delete(u)

    total = len(kfids)
    with ThreadPoolExecutor(max_workers=5) as tpex:
        for i, f in enumerate(
            tpex.map(delete, [f"{host}/{get_endpoint(k)}/{k}" for k in kfids])
        ):
            print(f"Deleted {i+1} of {total}: {f.url}")


def delete_entities(host, study_ids=None, safety_check=True):
    """
    Delete entities by study or delete all entities in Data Service. If
    study_ids is not provided, all entities in Data Service will be deleted.
    Default behavior only deletes resources at localhost unless
    safety_check=False

    Deletion is implemented in a way that avoids large cascading deletions in
    the Data Service database. Large cascading deletes are known to crash
    the Data Service. For example, first we delete genomic files, then
    biospecimens, and then participants rather than deleting participants first
    since that would cause a cascading delete of the specimens and their
    genomic files. The order in which entities are deleted is defined in
    ENDPOINTS.

    :param host: URL of the Data Service
    :type host: str
    :param study_ids: If provided, the entities linked to these study ids
     will be deleted, otherwise all entities in Data Service will be deleted
    :type study_ids: list of str
    :param saftey_check: Whether to delete if resource is not at localhost
    :type safety_check: bool
    """
    phrase = f"studies {pformat(study_ids)}" if study_ids else "all studies"
    print(f"Deleting {phrase} from {host}")

    if study_ids:
        # Delete entities by study id
        for study_id in study_ids:
            # Delete entities except "study" (it has to be handled differently)
            for endpoint in ENDPOINTS:
                print(f"Finding all {endpoint} from study {study_id}.")
                kfids = yield_kfids(
                    host, endpoint, {"study_id": study_id}, show_progress=True
                )
                if kfids:
                    print(f"Deleting all {endpoint} from study {study_id}.")
                    delete_kfids(host, kfids, safety_check=safety_check)
                else:
                    print(f"No {endpoint} found.")

            # Delete study by its kfid
            delete_kfids(host, [study_id], safety_check=safety_check)
    else:
        # Delete everything
        for endpoint in ENDPOINTS:
            print(f"Finding all {endpoint}.")
            kfids = yield_kfids(host, endpoint, {}, show_progress=True)
            if kfids:
                print(f"Deleting all {endpoint}.")
                delete_kfids(host, kfids, safety_check=safety_check)
            else:
                print(f"No {endpoint} found.")

        print(f"Finding all {STUDIES}.")
        kfids = yield_kfids(host, STUDIES, {}, show_progress=True)
        if kfids:
            print(f"Deleting all {STUDIES}.")
            delete_kfids(host, kfids, safety_check=safety_check)
        else:
            print(f"No {STUDIES} found.")
