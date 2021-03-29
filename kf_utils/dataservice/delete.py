import logging
from pprint import pformat
from urllib.parse import urlparse

import requests
from d3b_utils.requests_retry import Session

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
]
STUDIES = "studies"
LOCAL_HOSTS = {
    "localhost",
    "127.0.0.1",
}

logger = logging.getLogger(__name__)


def safe_delete(url, safety_check=True, session=None, **kwargs):
    """
    Only delete a non-local resource if safety_check is False.

    :param url: url of resource that will be deleted
    :type url: url
    :param saftey_check: Whether to delete if resource is not at localhost
    :type safety_check: bool
    :param session: requests session object to use when deleting
    :type session: requests.Session
    :param kwargs: Keyword args passed to requests.Session.delete
    :type kwarrgs: dict

    :returns: requests.Response from the delete
    :raises: Exception if safety_check=True and url is non-local
    """
    host = urlparse(url).netloc.split(":")[0]
    if safety_check and (host not in LOCAL_HOSTS):
        raise Exception(
            f"Cannot delete {url}. Safe delete is ENABLED. Resources that are "
            "not on localhost will not be deleted. You can try again with "
            "safe delete disabled."
        )

    session = session or Session()
    resp = session.delete(url, **kwargs)
    return resp


def delete_kfids(host, endpoint, kfids, safety_check=True):
    """
    Delete entities by KF ID. Default behavior only deletes resources at
    localhost unless safety_check=False

    :param host: URL of the Data Service
    :type host: str
    :param endpoint: Data Service endpoint
    :type endpoint: str
    :param kfids: Data Service Kids First IDs
    :type kfids: iterable of strs
    :param saftey_check: Whether to delete if resource is not at localhost
    :type safety_check: bool

    :returns: Dict of errors where Keys are urls that failed delete (non 200
    status code) and values are requests.Response objects
    """
    session = Session()
    errors = {}
    kf_ids = [kfid for kfid in kfids]
    total = len(kf_ids)
    for i, kf_id in enumerate(kf_ids):
        url = f"{host}/{endpoint.strip('/')}/{kf_id}"
        logger.info(f"Deleting {i+1} of {total}: {url}")
        resp = safe_delete(url, session=session, safety_check=safety_check)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.error(
                f"Failed to delete {url}, status code {resp.status_code}. "
                f"Response:\n{resp.text}"
            )
            errors[url] = resp
    return errors


def delete_entities(host, study_ids=None, safety_check=True):
    """
    Delete entities by study or delete all entities in Data Service. If
    study_ids is not provided, all entities in Data Service will be deleted.
    Default behavior only deletes resources at localhost unless
    safety_check=False

    Deletion is implemented in a way that avoids large cascading deletions in
    the Data Service database. Large cascading deletes are known to crash
    the Data Service. For example, first we delete genomic files, then
    biospecimens, and then participants rather than deleting participants
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

    :returns: Dict of errors where Keys are urls that failed delete (non 200
    status code) and values are requests.Response objects
    """
    phrase = f"studies {pformat(study_ids)}" if study_ids else "all studies"
    logger.info(f"Deleting {phrase} from {host}")

    # Get all study ids
    if not study_ids:
        study_ids = yield_kfids(host, "studies", {})

    # Delete entities by study id
    errors = {}
    for study_id in study_ids:
        # Delete entities except "study" (it has to be handled differently)
        for endpoint in ENDPOINTS:
            params = {"study_id": study_id}
            kfids = yield_kfids(host, endpoint, params)
            errors.update(
                delete_kfids(host, endpoint, kfids, safety_check=safety_check)
            )
        # Delete study by its kfid
        errors.update(
            delete_kfids(host, STUDIES, [study_id], safety_check=safety_check)
        )

    return errors
