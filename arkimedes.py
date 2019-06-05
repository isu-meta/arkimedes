import argparse

from arkimedes import get_xml_from_urls
from arkimedes.ead import generate_anvl_fields_from_ead_xml
from arkimedes.ezid import upload_anvl

# arkimedes un pass ark:/sjksk outfile.txt ead --anvil-file anvl.txt 1 2 3 ...

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("username")
    parser.add_argument("password")
    parser.add_argument("shoulder")
    parser.add_argument("output_file")
    parser.add_argument("type")
    parser.add_argument("--anvl-file")
    parser.add_argument("sources", nargs="+")

    args = parser.parse_args()

    if args.type == "ead":
        ead_xml = get_xml_from_urls(args.sources)

        if args.anvl_file:
            anvls = generate_anvl_fields_from_ead_xml(ead_xml, args.anvl_file)
        else:
            anvls = generate_anvl_fields_from_ead_xml(ead_xml)

        for anvl in anvls:
            upload_anvl(
                args.username, args.password, args.shoulder, anvl, args.output_file
            )
