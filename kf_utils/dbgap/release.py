import xmltodict
from d3b_utils.requests_retry import Session
from defusedxml import ElementTree as DefusedET
from defusedxml.common import DefusedXmlException


def get_latest_sample_status(phs_id, required_status="released"):
    """Get the most recently released sample status for a study on dbGaP

    :param phs_id: First part of study accession identifier, e.g. "phs001138"
    :type phs_id: string
    :raises Exception: if no released sample data can be found
    :return: full released accession id (e.g. phs001138.v3.p2), sample data
    :rtype: tuple
    """
    tried = {}
    version = None
    while True:
        phs_string = f"{phs_id}.v{version}" if version is not None else phs_id
        print(f"Querying dbGaP for study {phs_string}")
        url = (
            "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/"
            f"GetSampleStatus.cgi?study_id={phs_string}&rettype=xml"
        )
        data = Session(status_forcelist=(502, 503, 504)).get(url)
        if data.status_code != 200:
            tried[phs_string] = f"status {data.status_code}"
            raise Exception(
                f"Request for study {phs_id} failed."
                f" - Tried: {tried}"
            )

        try:
            safe_xml = DefusedET.tostring(DefusedET.fromstring(data.content))
        except DefusedXmlException as e:
            raise Exception(
                f"DETECTED UNSAFE XML -- {repr(e)} -- FROM {url}\n"
                "SEE: https://github.com/tiran/defusedxml"
            ).with_traceback(e.__traceback__)

        data = xmltodict.parse(safe_xml)
        study = data["DbGap"]["Study"]
        accession = study["@accession"]
        status = study["@registration_status"]

        if (required_status is None) or (status.lower() == required_status.lower()):
            break
        else:
            # try previous version
            version = int(accession.split(".")[1][1:]) - 1
            print(
                f"Study {accession} is not '{required_status}'. "
                f"registration_status: {status}"
            )
            tried[accession] = status

    return accession, study
