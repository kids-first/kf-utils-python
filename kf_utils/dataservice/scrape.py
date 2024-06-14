from concurrent.futures import ThreadPoolExecutor, as_completed

# from d3b_utils.requests_retry import Session
from requests import Session
from kf_utils.dataservice.meta import get_endpoint
from tqdm import tqdm


def yield_entities_from_filter(host, endpoint, filters, show_progress=False):
    """
    Scrape the dataservice for paginated entities matching the filter params.

    Note: It's almost always going to be safer to use this than requests.get
    with search parameters, because you never know when you'll get back more
    than one page of results for a query.

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

    found_kfids = set()
    which = {"limit": 100}
    expected = 0
    with tqdm(total=1, disable=not show_progress, leave=False) as pbar:
        while True:
            resp = Session().get(url, params={**which, **filters})

            if resp.status_code != 200:
                raise Exception(resp.text)

            j = resp.json()
            if j["total"] != expected:
                n = pbar.n
                pbar.reset(j["total"])
                pbar.update(n)

            expected = j["total"]
            res = j["results"]

            if not res:
                pbar.close()
            for entity in res:
                kfid = entity["kf_id"]
                if kfid not in found_kfids:
                    found_kfids.add(kfid)
                    pbar.update()
                    yield entity
            try:
                for key, i in [("after", 1), ("after_uuid", 2)]:
                    which[key] = j["_links"]["next"].split("=")[i].split("&")[0]
            except KeyError:
                break

    num = len(found_kfids)
    assert expected == num, f"FOUND {num} ENTITIES BUT EXPECTED {expected}"


def yield_entities_from_kfids(host, kfids, show_progress=False):
    """Fetch the given entities from the dataservice quickly.

    :param host: dataservice base url string (e.g. "http://localhost:5000")
    :param kfids: kfids to request entities for
    :raises Exception: if the dataservice doesn't return status 200
    :yields: entities for the given kfids
    """
    host = host.strip("/")

    quit = False

    def do_get(url):
        if quit:
            return
        response = Session().get(url)
        if response.status_code != 200:
            raise Exception(response.text)
        body = response.json()
        res = body["results"]
        res["_links"] = body["_links"]
        return res

    with ThreadPoolExecutor() as tpex:
        futures = [
            tpex.submit(do_get, f"{host}/{get_endpoint(k)}/{k}") for k in kfids
        ]
        with tqdm(total=len(kfids), disable=not show_progress) as pbar:
            try:
                for f in as_completed(futures):
                    pbar.update()
                    yield f.result()
            except KeyboardInterrupt:
                quit = True
                tpex.shutdown(wait=False)
                tpex._threads.clear()
                raise


def yield_entities(
    host, endpoint_if_filter, filters_or_kfids, show_progress=False
):
    """Combined call for yield_entities_from_filter and
    yield_entities_from_kfids to preserve backward compatibility because
    yield_entities_from_filter was previously called yield_entities.

    :param host: dataservice base url string (e.g. "http://localhost:5000")
    :param endpoint_if_filter: None or dataservice endpoint string
        (e.g. "genomic-files")
    :param filters_or_kfids: dict of filters to winnow results from the
        dataservice (e.g. {"study_id": "SD_DYPMEHHF", "external_id": "foo"})
        or a list of kfids
    :raises Exception: if the dataservice doesn't return status 200
    :yields: matching entities
    """
    if isinstance(filters_or_kfids, str):
        filters_or_kfids = [filters_or_kfids]

    if isinstance(filters_or_kfids, dict):
        assert endpoint_if_filter, "If filtering, must specify an endpoint"
        return yield_entities_from_filter(
            host,
            endpoint_if_filter,
            filters_or_kfids,
            show_progress=show_progress,
        )
    else:
        return yield_entities_from_kfids(
            host, filters_or_kfids, show_progress=show_progress
        )


def yield_kfids(host, endpoint, filters, show_progress=False):
    """Wrapper around yield_entities_from_filter that yields just KFIDs"""
    for e in yield_entities_from_filter(host, endpoint, filters, show_progress):
        yield e["kf_id"]
