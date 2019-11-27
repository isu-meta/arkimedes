"""A command-line script for managing ARKs with the arkimedes library

``arkimedes.py`` provides a command-line script for working with the 
ARKimedes Python library, a library created to manage the creation
and maintenance of ARKs for the Iowa State University Library.

Examples
--------
``arkimedes.py action target [username password]  [list of sources]``

All arkimedes.py commands require an action argument and a target argument.
All commands that interact with the EZID API require an EZID username and
password to be provided.


"""
import argparse

from arkimedes import get_sources
from arkimedes.db import (
    add_to_db,
    Ark,
    Base,
    create_db,
    db_exists,
    engine,
    find_all,
    find_ark,
    find_replaceable,
    find_url,
    Session,
    update_db_record,
    url_is_in_db,
)
from arkimedes.ead import generate_anvl_from_ead_xml
from arkimedes.ezid import (
    anvl_to_dict,
    batch_download,
    get_value_from_anvl_string,
    upload_anvl,
    view_anvl,
)
from arkimedes.pdf import generate_anvl_from_conservation_reports


class MissingArgumentError(Exception):
    pass


def add_ark_to_db(args, ark, anvl):
    ezid_anvl = view_anvl(args.username, args.password, ark, False)
    ark_obj = Ark()
    ark_obj.from_anvl(ezid_anvl)
    add_to_db(ark_obj)


def return_anvl(args):
    if args.source is not None:
        if "\n" in args.source:
            anvl = args.source
        else:
            with open(args.source, "r", encoding="utf-8") as fh:
                anvl = fh.read()
    else:
        raise MissingArgumentError(
            f"Action '{args.action}' requires '--source'."
        )

    return anvl


def submit_md(args, anvl=None):
    if anvl is None:
        anvl = return_anvl(args)

    if args.action == "update":
        ark = upload(args, anvl, "update")
    else:
        url = get_value_from_anvl_string("_target", anvl)

        if url_is_in_db(url):
            print(f"An ARK has already been minted for {url}.\n")
            ark_obj = find_url(url).first()
            view_anvl(args.username, args.password, ark_obj.ark)
        else:
            if args.reuse:
                replaceable = find_replaceable().first()

                if replaceable is not None:
                    args.target = replaceable.ark
                    upload(args, anvl, "update")
                    update_db_record(replaceable.ark, {"iastate.replaceable": False})
                else:
                    upload(args, anvl, "mint")
            else:
                ark = upload(args, anvl, "mint")


def upload(args, anvl, action):
    ark = upload_anvl(
        args.username, args.password, args.target, anvl, action, args.out
    )

    if action == "mint":
        add_ark_to_db(args, ark, anvl)
    else:
        anvl_dict = anvl_to_dict(anvl)
        update_db_record(ark, anvl_dict)
    return ark


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "action",
        help="""Action to take. Accepted arguments are: 'batch-download', 'delete',
'mint-anvl', 'mint-ead', 'mint-conservation-report', 'update', and 'view'.
'mint-anvl' mints new ARKs from ANVL metadata and 'mint-ead' mints ARKs 
from EAD XML. 'mint-conservation-report' mints new ARKs from Conservation
Report PDFs.""",
    )
    parser.add_argument(
        "target",
        help="""After any 'mint-' argument, this must be an ARK shoulder. After
'delete', 'update', or 'view', it must be an ARK. After 'batch' it may be
any character or characters; a hyphen is recommended.""",
    )
    parser.add_argument("username", nargs="?", default="", help="EZID username.")
    parser.add_argument("password", nargs="?", default="", help="EZID password.")
    parser.add_argument(
        "--batch-args",
        nargs="+",
        help="""Additional arguments for batch downloads in 'arg=value' format.
For a full list of available options, see: 
https://ezid.cdlib.org/doc/apidoc.html#parameters""",
    )
    parser.add_argument(
        "--batch-compression",
        help="""File compression to use with batch downloads. Accepted values are
'gzip' and 'zip' If this argument is not given, the default 'zip' is used.""",
    )
    parser.add_argument(
        "--batch-format",
        help="""The file format to be returned when batch downloading ARK records.
Accepted values are 'anvl', 'csv', and 'xml'. If this argument is not given,
the default format 'anvl' is used.""",
    )
    parser.add_argument(
        "--out", help="Output file for recording EZID API response."
    )
    parser.add_argument(
        "--source",
        nargs="+",
        help="Files or URLs that contain metadata for populating ARK records",
    )
    parser.add_argument(
        "--reuse",
        action="store_true",
        help="""When this flag is used with an action that mints a new ARK,
arkimedes will reuse an ARK record that's been marked replaceable, if one
is available, rather than minting a new ARK. If no replaceable record is 
available, a new ARK will be minted.""",
    )

    args = parser.parse_args()

    if not db_exists():
        create_db(engine)

    if args.action == "mint-anvl":
        submit_md(args)
    elif args.action == "mint-ead":
        ead_xml = get_sources(args.source)

        if args.out:
            anvls = generate_anvl_from_ead_xml(ead_xml, args.out)
        else:
            anvls = generate_anvl_from_ead_xml(ead_xml)

        for anvl in anvls:
            submit_md(args, anvl)
    elif args.action == "mint-conservation-report":
        pdfs = get_sources(args.sources)
        pdf_data = zip(pdfs, args.sources)

        if args.out:
            anvls = generate_anvl_from_conservation_reports(pdf_data, args.out)
        else:
            anvls = generate_anvl_from_conservation_reports(pdf_data)

        for anvl in anvls:
            submit_md(args, anvl)
    elif args.action == "batch-download":
        format_ = "anvl"
        compression = "zip"

        if args.batch_format is not None:
            format_ = args.batch_format

        if args.batch_compression is not None:
            compression = args.batch_compression

        if args.batch_args is not None:
            batch_download(
                args.username, args.password, format_, compression, args.batch_args
            )
        else:
            batch_download(args.username, args.password, format_, compression)

    elif args.action == "delete":
        pass
    elif args.action == "update":
        submit_md(args)
    elif args.action == "view":
        view_anvl(args.username, args.password, args.target)


if __name__ == "__main__":
    main()
