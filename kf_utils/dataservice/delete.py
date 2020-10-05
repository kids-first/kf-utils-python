import argparse
import logging
import logging.handlers
import os

from d3b_utils.requests_retry import Session

QA = 'https://kf-api-dataservice-qa.kidsfirstdrc.org'
PROD = 'https://kf-api-dataservice.kidsfirstdrc.org'
LOCAL = 'http://localhost:5000'
DEFAULT_LOG_FILENAME = 'deletion.log'

logger = None


def setup_logger(log_filename=DEFAULT_LOG_FILENAME, log_level=logging.INFO):
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s" " - %(levelname)s - %(message)s"
    )
    # Log file
    filename = DEFAULT_LOG_FILENAME
    log_filepath = os.path.join(os.getcwd(), filename)

    # Setup rotating file handler
    fileHandler = logging.handlers.RotatingFileHandler(log_filepath, mode="w")
    fileHandler.setFormatter(formatter)

    # Setup console handler
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)

    # Set log level and handlers
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(fileHandler)
    logger.addHandler(consoleHandler)


def get_total(url, params=None):
    """
    Get total entities at url
    """
    return Session().get(url, params=params).json()['total']


def yield_entities(host, endpoint, filters):
    """
    Modified version in kf-utils-python

    Scrape the dataservice for paginated entities matching the filter params.

    :param host: dataservice base url string (e.g. "http://localhost:5000")
    :param endpoint: dataservice endpoint string (e.g. "genomic-files")
    :param filters: dict of filters to winnow results from the dataservice
        (e.g. {"study_id": "SD_DYPMEHHF", "external_id": "foo"})
    :raises Exception: if the dataservice doesn't return status 200
    :yields: entities matching the filters
    """
    host = host.strip("/")
    endpoint = endpoint.strip("/")
    url = f"{host}/{endpoint}"

    which = {"limit": 100}
    count = 0
    expected = get_total(url, params={**which, **filters})
    if expected == 0:
        logger.info(f'⚠️ Found 0 entities at {endpoint}!')
        return
    else:
        logger.info('')
        logger.info(
            f'--- Fetching {expected} {endpoint} entities ----'
        )

    while True:
        resp = Session().get(url, params={**which, **filters})

        if resp.status_code != 200:
            raise Exception(resp.text)

        j = resp.json()

        for entity in j["results"]:
            count += 1
            logger.info(
                f'➡️  Fetched {url}/{entity["kf_id"]}, {count} of {expected}'
            )
            yield entity
        try:
            for (key, i) in [("after", 1), ("after_uuid", 2)]:
                which[key] = j["_links"]["next"].split("=")[i].split("&")[0]
        except KeyError:
            break


def yield_kfids(host, endpoint, filters):
    """Simple wrapper around yield_entities that yields just the KFIDs"""
    for e in yield_entities(host, endpoint, filters):
        yield e["kf_id"]


def delete_all(host, endpoint, params=None, stop_at_count=None):
    """
    Delete all entities at host/endpoint. Forward params on to yield_kfids
    If stop_at_count supplied, stop deleting when we've reached stop_at_count
    """
    logger.info(f'Deleting {endpoint} ...')

    for i, kf_id in enumerate(yield_kfids(host, endpoint, params)):
        if stop_at_count and (i >= stop_at_count):
            logger.info(f'Reached stop_at_count: {stop_at_count}, exiting..')
            break

        url = f"{host}/{endpoint.strip('/')}/{kf_id}"
        Session().delete(url)
        logger.info(f'🗑 Deleted {url}')


def delete_all_except_se_and_gfs(host, study_id, stop_at_count=None):
    """
    Delete all entities within a study except for its sequencing experiments,
    and genomic files:

    1. Unlink biospecimens from genomic files
        - Delete records from biospecimen-genomic-file
    2. Try deleting rest of entities by deleting families
    3. If no families exist, delete participants instead. This will cascade
    delete all of the entities in the study except genomic files.

    Verify that we deleted everything we wanted to at the end
    """
    params = {"study_id": study_id}

    gfs_before = get_total(f"{host}/genomic-files")
    se_before = get_total(f"{host}/sequencing-experiments")

    # Delete all biospecimen-genomic files for this study - -
    delete_all(
        host, 'biospecimen-genomic-files',
        params=params, stop_at_count=stop_at_count
    )
    # Try deleting via family first
    delete_all(
        host, 'families',
        params=params, stop_at_count=stop_at_count
    )
    # If study has 0 families, then try deleting via participants
    delete_all(
        host, 'participants',
        params=params, stop_at_count=stop_at_count
    )

    def verify(endpoint, before, after):
        assert before == after, (
            f'❌ Before delete {endpoint} count: {before}, '
            f'after delete {endpoint} count {after}!'
        )

    # Make sure we didn't delete any genomic files
    endpoint = 'genomic-files'
    gfs_after = get_total(f"{host}/{endpoint}")
    verify(endpoint, gfs_before, gfs_after)

    # Make sure we didn't delete any sequencing experiments
    endpoint = 'sequencing-experiments'
    se_after = get_total(f"{host}/{endpoint}")
    verify(endpoint, se_before, se_after)

    # Check other stuff is done
    endpoints = [
        'participants', 'biospecimens', 'diagnoses', 'outcomes', 'phenotypes'
    ]
    for endpoint in endpoints:
        after = get_total(f"{host}/{endpoint}", params=params)
        assert after == 0, (
            f"Found {after} {endpoint}, expected 0!"
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('study_id', help='KF ID of study')
    parser.add_argument(
        '--host', help='URL of host, e.g. http://localhost:5000'
    )
    args = parser.parse_args()

    stop_at_count = None
    host = args.host or LOCAL
    study_id = args.study_id

    setup_logger(
        f'{study_id}-{DEFAULT_LOG_FILENAME}',
        log_level=logging.INFO
    )
    logger = logging.getLogger(__name__)
    try:
        delete_all_except_se_and_gfs(
            host,
            study_id,
            stop_at_count=stop_at_count
        )
    except Exception as e:
        logger.exception(str(e))
        logger.info('❌ Something went wrong! Exiting.')
        exit(1)

    logger.info('✅ Deletion complete')
