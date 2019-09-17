from io import BytesIO

from lxml import etree

from arkimedes import CONCAT_STRING
from arkimedes.ezid import build_anvl
from arkimedes.qa import check_lc_naf


def generate_anvl_fields_from_ead_xml(files, output_file=None):
    anvl_strings = []

    for xml in files:
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

            anvl_strings.append(build_anvl(creator, title, dates, target))
        except etree.XMLSyntaxError:
            print(etree.XMLSyntaxError)
            with open("malformed_xml.txt", "a", encoding="utf-8") as fh:
                fh.write(xml.decode("utf-8"))
                fh.write(CONCAT_STRING)

    if output_file:
        with open(output_file, "w", encoding="utf-8") as fh:
            fh.write(CONCAT_STRING.join(anvl_strings))

    return anvl_strings
