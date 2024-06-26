prefix_endpoints = {
    "AG": "alias-groups",
    "BD": "biospecimen-diagnoses",
    "BG": "biospecimen-genomic-files",
    "BS": "biospecimens",
    "CA": "cavatica-apps",
    "DG": "diagnoses",
    "FM": "families",
    "FR": "family-relationships",
    "GF": "genomic-files",
    "IG": "investigators",
    "OC": "outcomes",
    "PH": "phenotypes",
    "PT": "participants",
    "RF": "read-group-genomic-files",
    "RG": "read-groups",
    "SA": "samples",
    "SC": "sequencing-centers",
    "SD": "studies",
    "SE": "sequencing-experiments",
    "SF": "study-files",
    "SG": "sequencing-experiment-genomic-files",
    "SR": "sample-relationships",
    "TG": "task-genomic-files",
    "TK": "tasks",
}


def prefix(kfid):
    return kfid.split("_")[0]


def get_endpoint(kfid):
    return prefix_endpoints[prefix(kfid)]
