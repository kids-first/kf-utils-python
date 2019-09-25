# Collection of reusable python utilities

## Requires

Python >= 3.6

## How to install

Using pip

`pip install -e git+ssh://git@github.com/kids-first/kf-utils-python.git#egg=kf_utils`

## Included so far

### Kids First dataservice query scraper

```Python
from kf_utils.dataservice_scrape import yield_entities

for e in yield_entities(
    kf_api_url, "participants", {"study_id": "SD_1234567"}
):
  ...
```

The Kids First dataservice paginates its responses and can only return up
to 100 results per page. This simplifies the process of retrieving all
of the entities from all of the pages for a given query.
