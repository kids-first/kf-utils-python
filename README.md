# Collection of reusable python utilities

## Requires

Python >= 3.6

## How to install

Using pip

`pip install -e git+ssh://git@github.com/kids-first/kf-utils-python.git#egg=kf_utils`

## Included so far

n.b. View individual files for informative docstrings and other usage comments.

### Dataservice

#### [dataservice/scrape.py](kf_utils/dataservice/scrape.py) - Query scraping

The Kids First dataservice paginates its responses and can only return up
to 100 results per page. This simplifies the process of retrieving all
of the entities from all of the pages for a given query.

```Python
from kf_utils.dataservice.scrape import yield_entities

for e in yield_entities(
    kf_api_url, "participants", {"study_id": "SD_12345678"}
):
  ...
```

#### [dataservice/descendants.py](kf_utils/dataservice/descendants.py) - Descendant entity discovery

Entities in the dataservice are linked to each other by descendancy
relationships. Participants descend from families. Biospecimens/phenotypes/etc
descend from participants. Genomic files descend from biospecimens.

When we change a set of entities (e.g. hiding or unhiding), we may also want to
change their descendant entities.

```Python
from kf_utils.dataservice.descendants import (
    find_descendants_by_kfids,
    find_descendants_by_filter,
    find_descendant_genomic_files_with_extra_contributors
)

host = "https://kf-api-dataservice.kidsfirstdrc.org"

# Get descendant entities for these families, including any genomic files that
# are only partially composed of these families' biospecimens
d1 = find_descendants_by_kfids(
    host, "families", ["FM_11111111", "FM_22222222", "FM_33333333"],
    ignore_gfs_with_hidden_external_contribs=False
)
```

```Python
# Get descendant entities for hidden families in SD_DYPMEHHF, but only include
genomic files with other contributing biospecimens if those specimens are visible
d2 = find_descendants_by_filter(
    host, "families", {"study_id": "SD_DYPMEHHF", "visible": False},
    ignore_gfs_with_hidden_external_contribs=True
)
```

```Python
# List genomic files with contributions from these biospecimens that also have 
# contributions from biospecimens that aren't these
promiscuous_gs = find_gfs_with_extra_contributors(
  host, ["BS_11111111", "BS_22222222", "BS_33333333"]
)
```

```Python
# Hide all visible families in study SD_DYPMEHHF and all of their descendants.
# This and show_all_descendants_by_filter are not symmetrical.
hide_all_descendants_by_filter(host, "families", {"study_id": "SD_DYPMEHHF", "visible": True})
```

```Python
# Unhide all hidden families in study SD_DYPMEHHF and all of their descendants except for
# genomic files with additional contributing specimens if those specimens will remain 
# hidden.
# This and hide_all_descendants_by_filter are not symmetrical.
show_all_descendants_by_filter(host, "families", {"study_id": "SD_DYPMEHHF", "visible": False})
```
#### [dataservice/patch.py](kf_utils/dataservice/patch.py) - Rapid patch submission

Streamline patching the dataservice quickly.

```Python
from kf_utils.dataservice.patch import (
    send_patches,
    patch_things_with_func,
    hide_kfids,
    show_kfids
)

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

# Patch the given KFIDs according to the result of the function
patch_things_with_func(host, things, my_patch_func)
```

```Python
# Hide the given KFIDs
hide_kfids(host, ["PT_12345678", "BS_99999999"])
```

```Python
# Show the given KFIDs
show_kfids(host, ["PT_12345678", "BS_99999999"])
```
