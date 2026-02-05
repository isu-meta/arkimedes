from io import BytesIO

from lxml import etree
from sickle import Sickle

from arkimedes.ezid import build_anvl, generate_anvl_strings
from arkimedes.qa import check_lc_naf

CONCAT_STRING = "\n---\n"


def get_oai_ead(url):
    """Fetch record from OAI-PMH endpoint return XML as byte string.

    Parameters
    ----------
    url : str
        The URL for a finding aid.

    Returns:
    --------
    bytes
        UTF-8-encoded XML byte string
    """
    oai_pmh_endpoint = "https://cardinal.lib.iastate.edu/oai"
    sickle = Sickle(oai_pmh_endpoint)
    identifier_prefix = "oai:archivesspace:"
    url_prefix = "https://cardinal.lib.iastate.edu"
    obj_path = url.replace(url_prefix, "")

    oai_id = f"{identifier_prefix}{obj_path}"

    return sickle.GetRecord(
        identifier=oai_id, metadataPrefix="oai_ead"
    ).raw.encode("utf8")


def _generate_anvl_from_ead_xml(xml):
    """Generate ANVL string from XML bytes string.

    Parameter
    ---------
    xml : bytes

    Returns
    -------
    str or None
    """
    try:
        ns = {"ead": "urn:isbn:1-931666-22-9"}
        tree = etree.parse(BytesIO(xml))
        creator = check_lc_naf(
            tree.xpath(
                "string(//ead:archdesc/ead:did/ead:origination[@label='Creator']/*[self::ead:persname or self::ead:corpname]/text())",
                namespaces=ns,
            )
        )

        title = tree.xpath(
            "string(//ead:archdesc/ead:did/ead:unittitle/text())",
            namespaces=ns,
        )

        dates = tree.xpath(
            "string(//ead:archdesc/ead:did/ead:unitdate/@normal)",
            namespaces=ns,
        )

        target_path = tree.xpath(
            "string(//ead:archdesc/ead:did/ead:unitid[@type='aspace_uri'])",
            namespaces=ns,
        )

        target = f"https://cardinal.lib.iastate.edu{target_path}"

        return build_anvl(
            {
                "dc.creator": creator,
                "dc.title": title,
                "dc.date": dates,
                "_target": target,
            }
        )
    except etree.XMLSyntaxError:
        print(etree.XMLSyntaxError)
        with open("malformed_xml.txt", "a", encoding="utf-8") as fh:
            fh.write(xml.decode("utf-8"))
            fh.write(CONCAT_STRING)


def generate_anvl_from_ead_xml(xmls, output_file=None):
    """Generate ANVL strings from EAD XML bytes strings

    Parameters
    ----------
    xmls : Iterable[bytes] or Iterator[bytes]

    output_file : str or Path or None

    Returns
    -------
    list[str]
       List of ANVL-formatted strings
    """
    return generate_anvl_strings(
        xmls, _generate_anvl_from_ead_xml, output_file
    )
