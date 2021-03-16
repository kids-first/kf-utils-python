from collections import defaultdict

from d3b_utils.aws_bucket_contents import fetch_bucket_obj_info
from kf_utils.dataservice.scrape import yield_entities


def merge_s3_and_kf_gfs(
    ds_url, study_kfid, study_bucket, exclude_s3_keypaths=None
):
    """Return file data from S3 and the Kids First dataservice merged together
    on external_id to see which S3 files have been loaded into the data service
    and which loaded files no longer exist.

    Note: You must be able to query both S3 and the dataservice
        (VPN + chopaws if running locally)

    :param study_kfid: Dataservice KFID of the study
    :type study_kfid: string
    :param study_bucket: Amazon S3 bucket containing study files
    :type study_bucket: string
    :param exclude_s3_keypaths: S3 paths starting with these strings will be
        excluded, optional, defaults to None
    :type exclude_s3_keypaths: string, iterable
    :return: list of dicts
    """
    # Files from the dataservice
    # We use the API because direct DB queries won't give us the gen3 fields
    kf = {
        e["external_id"]: {
            f"kf_{k.lower()}": v
            for k, v in e.items()
            if k not in ["_links", "access_urls", "urls"]
        }
        for e in yield_entities(
            ds_url,
            "genomic-files",
            {"study_id": study_kfid},
            show_progress=True,
        )
    }

    # Files from S3
    s3 = {
        "s3://"
        + o["Bucket"]
        + "/"
        + o["Key"]: {f"s3_{k.lower()}": v for k, v in o.items()}
        for o in fetch_bucket_obj_info(
            study_bucket,
            drop_folders=True,
        )
    }

    # Sadly it's muuuuch harder to exclude paths on the S3 request side because
    # the S3 API doesn't support it. So we're stuck for now waiting for
    # potentially thousands of pagination requests that we don't care about,
    # and then we remove them here.
    if exclude_s3_keypaths:
        if isinstance(exclude_s3_keypaths, str):
            exclude_s3_keypaths = (exclude_s3_keypaths,)
        elif exclude_s3_keypaths is not None:
            exclude_s3_keypaths = tuple(exclude_s3_keypaths)

        s3 = {
            k: v
            for k, v in s3.items()
            if not v["s3_key"].startswith(exclude_s3_keypaths)
        }

    # Merge them together
    s3kf = defaultdict(dict, s3)
    for k, v in kf.items():
        s3kf[k].update(v)

    return list(s3kf.values())
