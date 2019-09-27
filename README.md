# Collection of reusable python utilities

## Requires

Python >= 3.6

## How to install

Using pip

`pip install -e git+ssh://git@github.com/kids-first/kf-utils-python.git#egg=kf_utils`

## Included so far

### Dataservice query scraper

The Kids First dataservice paginates its responses and can only return up
to 100 results per page. This simplifies the process of retrieving all
of the entities from all of the pages for a given query.

```Python
from kf_utils.dataservice_scrape import yield_entities

for e in yield_entities(
    kf_api_url, "participants", {"study_id": "SD_12345678"}
):
  ...
```

### Dataservice descendant entity finder

Entities in the dataservice are linked to each other by descendancy
relationships. Participants descend from families. Biospecimens/phenotypes/etc
descend from participants. Genomic files descend from biospecimens.

When we change a set of entities (e.g. hiding or unhiding), we may also want to
change their descendant entities.

```Python
from kf_utils.dataservice_descendants import (
    find_descendants_by_kfids,
    find_descendants_by_filter,
    find_descendant_genomic_files_with_extra_contributors
)

host = "https://kf-api-dataservice.kidsfirstdrc.org"

# Get descendant entities for these families
d1 = find_descendants_by_kfids(
    host, "families", ["FM_11111111", "FM_22222222", "FM_33333333"],
    include_gfs_with_external_contributors=False
)

# get descendant entities for visible families in SD_DYPMEHHF
d2 = find_descendants_by_filter(
    host, "families", {"study_id": "SD_DYPMEHHF", "visible": True},
    include_gfs_with_external_contributors=False
)

# list genomic files with contributions from these biospecimens that also have contributions from biospecimens that aren't these
promiscuous_gs = find_descendant_genomic_files_with_extra_contributors(
  host, ["BS_11111111", "BS_22222222", "BS_33333333"]
)
```
