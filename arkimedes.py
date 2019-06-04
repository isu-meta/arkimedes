import argparse

from arkimedes import get_xml_from_urls
from arkimedes.ead import generate_anvl_fields_from_ead_xml
from arkimedes.ezid import upload_anvl

# arkimedes un pass ark:/sjksk outfile ead --anvil-file 1 2 3 ...

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

    print(args)

    if args.type == "ead":
        ead_xml = get_xml_from_urls(args.sources)
        # print(ead_xml)

        if args.anvl_file:
            anvls = generate_anvl_fields_from_ead_xml(ead_xml, args.anvl_file)
        else:
            anvls = generate_anvl_fields_from_ead_xml(ead_xml)

        for anvl in anvls:
            print(anvl)
            upload_anvl(
                args.username, args.password, args.shoulder, anvl, args.output_file
            )
