# Collection of reusable python utilities

## Requires

Python >= 3.6

## How to install

Using pip

`pip install git+https://github.com/kids-first/kf-utils-python.git`

## Included so far

n.b. View individual files for informative docstrings and other usage comments.

### Dataservice

#### [dataservice/delete.py](kf_utils/dataservice/delete.py) - Study deletion

```Python
from kf_utils.dataservice.delete import delete_entities
```

```Python
# Delete all studies in Dataservice (raises Exception on deletion of non-local resources)
delete_entities(kf_api_url, study_ids=None)
```

```Python
# Delete a study in Dataservice (raises Exception on deletion of non-local resources)
delete_entities(kf_api_url, study_ids=["SD_12345678"])
```

```Python
# Delete a study in Dataservice (allows deletion of non-local resources)
delete_entities(kf_api_url, study_ids=["SD_12345678"], safety_check=False)
```

#### [dataservice/scrape.py](kf_utils/dataservice/scrape.py) - Query scraping

The Kids First dataservice paginates its responses and can only return up
to 100 results per page. This simplifies the process of retrieving all
of the entities from all of the pages for a given query.

```Python
from kf_utils.dataservice.scrape import *
```

```Python
# Yield all entities from the given endpoint matching the given filter query
for e in yield_entities_from_filter(
    kf_api_url, "participants", {"study_id": "SD_12345678"}, show_progress=True
):
  ...
```

```Python
# Yield all entities matching the given kfids
for e in yield_entities_from_kfids(
    kf_api_url, ["PT_12345678", "BS_99999999"], show_progress=True
):
  ...
```

```Python
# Compatibility wrapper for yielding entities from kfid or a filter query
for e in yield_entities(
    kf_api_url, "participants", {"study_id": "SD_12345678"}, show_progress=True
):
  ...

for e in yield_entities(
    kf_api_url, None, ["PT_12345678", "BS_99999999"], show_progress=True
):
  ...
```

```Python
# Like yield_entities_from_filter but just yields the kfids
for kfid in yield_kfids(
    kf_api_url, "participants", {"study_id": "SD_12345678"}, show_progress=True
):
  ...
```

#### [dataservice/descendants.py](kf_utils/dataservice/descendants.py) - Descendant entity discovery

Entities in the dataservice are linked to each other by descendancy
relationships. Participants descend from families. Biospecimens/phenotypes/etc
descend from participants. Genomic files descend from biospecimens.

When we change a set of entities (e.g. hiding or unhiding), we may also want to
change their descendant entities.

**NOTE: Where possible below, using the direct DB access URL will result in _much_ faster operation.**

```Python
from kf_utils.dataservice.descendants import *
```

```Python
api_url = "https://kf-api-dataservice.kidsfirstdrc.org"
db_url = f"postgres://{USER_NAME}:{PASSWORD}@kf-dataservice-api-prd-2019-9-11.c3siovbugjym.us-east-1.rds.amazonaws.com:5432/kfpostgresprd"

# Get descendant entities for these families, including any genomic files that
# are only partially composed of these families' biospecimens
d1 = find_descendants_by_kfids(
    db_url or api_url, "families", ["FM_11111111", "FM_22222222", "FM_33333333"],
    ignore_gfs_with_hidden_external_contribs=False, kfids_only=False
)
```

```Python
# Get descendant kfids for hidden families in SD_DYPMEHHF, but only include
# genomic files with other contributing biospecimens if those specimens are visible
d2 = find_descendants_by_filter(
    api_url, "families", {"study_id": "SD_DYPMEHHF", "visible": False},
    ignore_gfs_with_hidden_external_contribs=True, kfids_only=True, db_url=db_url
)
```

```Python
# List genomic files with contributions from these biospecimens that also have
# contributions from biospecimens that aren't these
promiscuous_gs = find_gfs_with_extra_contributors(
    db_url or api_url, ["BS_11111111", "BS_22222222", "BS_33333333"]
)
```

```Python
# Hide all visible families in study SD_DYPMEHHF and all of their descendants.
# Genomic files receive the specified acl.
# This and unhide_descendants_by_filter are not symmetrical.
hide_descendants_by_filter(
    api_url, "families", {"study_id": "SD_DYPMEHHF", "visible": True}, gf_acl=["SD_DYPMEHHF", "phs001436.c999"], db_url=db_url, dry_run=False
)
```

```Python
# Unhide all hidden families in study SD_DYPMEHHF and all of their descendants except for
# genomic files with additional contributing specimens if those specimens will remain
# hidden.
# This and hide_descendants_by_filter are not symmetrical.
unhide_descendants_by_filter(
    api_url, "families", {"study_id": "SD_DYPMEHHF", "visible": False}, db_url=db_url, dry_run=False
)
```

`descendants.py` also provides wrapper functions hiding/unhiding descendants by KF ID(s):

```Python
# Hide these families and all of their descendants. Genomic files receive the
# specified acl.
# This and unhide_descendants_by_kfids are not symmetrical.
hide_descendants_by_kfids(
    api_url, "families", ["FM_12345678", "FM_87654321"], gf_acl=["SD_DYPMEHHF", "phs001436.c999"], db_url=db_url, dry_run=False
)
```

```Python
# Unhide these families and all of their descendants except for genomic files with
# additional contributing specimens if those specimens will remain hidden.
# This and hide_descendants_by_kfids are not symmetrical.
unhide_descendants_by_kfids(
    api_url, "families", ["FM_12345678", "FM_87654321"], db_url=db_url, dry_run=False
)
```

#### [dataservice/patch.py](kf_utils/dataservice/patch.py) - Rapid patch submission

Streamline patching the dataservice quickly.

```Python
from kf_utils.dataservice.patch import *
```

```Python
host = "http://localhost:5000"

# Patch the given KFIDs with the given changes
patches = {
    "PT_12345678": {"visible": True},
    "BS_99999999": {"participant_id": "PT_12345678", "visible": False}
}
send_patches(host, patches)
```

```Python
things = [
    {"kf_id": "PT_11223344", "visible": True},
    {"kf_id": "PT_22334455", "visible": False}
]

def my_patch_func(thing):
    return {"external_id": "VISIBLE" if thing["visible"] else "HIDDEN"}

# Patch the given KFIDs or entities according to the result of the function
patch_things_with_func(host, things, my_patch_func)
```

```Python
# Hide the given KFIDs and assign an empty acl to the hidden genomic file
hide_kfids(host, ["PT_12345678", "GF_99999999"], gf_acl=[])
```

```Python
# Unhide the given KFIDs
unhide_kfids(host, ["PT_12345678", "BS_99999999"])
```

#### [dataservice/reporting.py](kf_utils/dataservice/reporting.py) - Generating reports

```Python
from kf_utils.dataservice.reporting import *
```

```Python
# Generate a report on which S3 files have been loaded into the data service
# and which loaded files no longer exist on S3 by merging the details together.

ds_url = "https://kf-api-dataservice.kidsfirstdrc.org"
study_kfid = "SD_12345678"
study_bucket = "kf-study-us-east-1-prd-sd-12345678"
exclude_s3_keypaths = ("path/with_files/I_don't_care_about", "another/one")

# This can take a while to run because of API limitations.
output = merge_s3_and_kf_gfs(
    ds_url, study_kfid, study_bucket, exclude_s3_keypaths
)
```

### dbGaP

#### [dbgap/release.py](kf_utils/dbgap/release.py) - dbGaP release XML scraping

```Python
from kf_utils.dbgap.release import get_latest_released_sample_status
```

```Python
# Get the sample status data for the latest "released" version of this study
versioned_accession, study_data = get_latest_released_sample_status("phs001138")
```
