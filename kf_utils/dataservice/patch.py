from concurrent.futures import ThreadPoolExecutor, as_completed

from d3b_utils.requests_retry import Session
from kf_utils.dataservice.meta import prefix, prefix_endpoints


def send_patches(host, patches):
    """
    Rapidly submit patch requests to the server.

    :param host: dataservice base url string (e.g. "http://localhost:5000")
    :param patches: dict mapping KFIDs to patch dicts
    :raises Exception: if server doesn't respond OK
    """

    def do_patch(url, patch):
        msg = f"Patching {url} with {patch}"
        resp = Session().patch(url, json=patch)
        if not resp.ok:
            raise Exception(f"{resp.status_code} -- {msg} -- {resp.json()}")
        return msg

    with ThreadPoolExecutor() as tpex:
        futures = []
        for kfid, patch in patches.items():
            endpoint = prefix_endpoints[prefix(kfid)]
            url = f"{host}/{endpoint}/{kfid}"
            futures.append(tpex.submit(do_patch, url, patch))
        for f in as_completed(futures):
            print(f.result())


def patch_things_with_func(host, things, patch_func):
    """
    Patch a set of entities using a custom function.

    :param host: dataservice base url string (e.g. "http://localhost:5000")
    :param things: list of entities or KFIDs to patch
    :param patch_func: function that receives an entity or KFID and returns
        a dict to patch with based on the value or contents
    """
    patches = {
        t["kf_id"] if isinstance(t, dict) else t: patch_func(t) for t in things
    }
    send_patches(host, patches)


def hide_kfids(host, kfid_list, gf_acl=None):
    """
    Hide a set of KFIDs

    :param host: dataservice base url string (e.g. "http://localhost:5000")
    :param kfid_list: list of kfids to hide
    :param gf_acl: acl to set when hiding any genomic files
    """

    def hide_function(k):
        if prefix_endpoints[prefix(k)] == "genomic-files":
            return {"visible": False, "acl": gf_acl or []}
        else:
            return {"visible": False}

    patch_things_with_func(host, kfid_list, hide_function)


def unhide_kfids(host, kfid_list):
    """
    Unhide a set of KFIDs

    :param host: dataservice base url string (e.g. "http://localhost:5000")
    :param kfid_list: list of kfids to unhide
    """
    patch_things_with_func(host, kfid_list, lambda x: {"visible": True})
