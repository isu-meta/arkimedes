from io import BytesIO

from lxml import etree

from arkimedes.ezid import build_anvl, generate_anvl_strings
from arkimedes.qa import check_lc_naf

CONCAT_STRING = "\n---\n"


def _generate_anvl_from_ead_xml(xml):
    try:
        tree = etree.parse(BytesIO(xml))
        try:
            creator = check_lc_naf(
                tree.xpath(
                    "//archdesc/did/origination[@label='Creator']/*[self::persname or self::corpname]/text()"
                )[0]
            )
        except IndexError:
            creator = ""

        title = tree.xpath("//archdesc/did/unittitle/text()")[0]

        try:
            dates = tree.xpath("//archdesc/did/unitdate/@normal")[0]
        except:
            dates = ""

        target = tree.xpath("//ead/eadheader/eadid/text()")[0]

        return build_anvl(creator, title, dates, target)
    except etree.XMLSyntaxError:
        print(etree.XMLSyntaxError)
        with open("malformed_xml.txt", "a", encoding="utf-8") as fh:
            fh.write(xml.decode("utf-8"))
            fh.write(CONCAT_STRING)


def generate_anvl_from_ead_xml(xml, output_file=None):
    return generate_anvl_strings(xml, _generate_anvl_from_ead_xml, output_file)
