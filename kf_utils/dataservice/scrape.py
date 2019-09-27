from d3b_utils.requests_retry import Session


def yield_entities(host, endpoint, filters, show_progress=False):
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
    while True:
        resp = Session().get(url, params={**which, **filters})

        if resp.status_code != 200:
            raise Exception(resp.text)

        j = resp.json()
        res = j["results"]
        expected = j["total"]

        for entity in res:
            kfid = entity["kf_id"]
            if kfid not in found_kfids:
                found_kfids.add(kfid)
                if show_progress:
                    print(".", end="", flush=True)
                yield entity
        try:
            for (key, i) in [("after", 1), ("after_uuid", 2)]:
                which[key] = j["_links"]["next"].split("=")[i].split("&")[0]
        except KeyError:
            break

    num = len(found_kfids)
    assert expected == num, f"FOUND {num} ENTITIES BUT EXPECTED {expected}"


def yield_kfids(host, endpoint, filters, show_progress=False):
    """Simple wrapper around yield_entities that yields just the KFIDs"""
    for e in yield_entities(host, endpoint, filters, show_progress):
        yield e["kf_id"]
