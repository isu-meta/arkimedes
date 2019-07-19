"""A command-line script for managing ARKs with the arkimedes library

``arkimedes.py`` provides a command-line script for working with the 
ARKimedes Python library, a library created to manage the creation
and maintenance of ARKs for the Iowa State University Library.

Examples
--------
``arkimedes.py username password action ark:/99999  [list of sources]``

All arkimedes.py commands require an EZID username and password to be
provided as the first two arguments, followed by an action and, depending
on the action specified, either an ARK or an ARK shoulder. If minting
new ARKs, a list of one or more metadata sources is required to generate
the needed ARK records.
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
    Session,
    url_is_in_db,
)
from arkimedes.ead import generate_anvl_fields_from_ead_xml
from arkimedes.ezid import batch_download, upload_anvl, view_anvl


class MissingArgumentError(Exception):
    pass


def return_anvl(args):
    if args.anvl is not None:
        anvl = args.anvl
    elif args.anvl_in is not None:
        with open(args.anvl_in, "r", encoding="utf-8") as fh:
            anvl = fh.read()
    else:
        raise MissingArgumentError(
            f"Action '{args.action}' requires either '--anvl' or '--anvl-in'."
        )

    return anvl


def submit_md(args):
    anvl = return_anvl(args)

    if args.action == "update":
        ark = upload_anvl(
            args.username, args.password, args.ark, anvl, "update", args.output_file
        )
    else:
        ark = upload_anvl(
            args.username, args.password, args.ark, anvl, "mint", args.output_file
        )
    # add item to db
    ezid_anvl = view_anvl(args.username, args.password, ark, False)
    ark_obj = Ark()
    ark_obj.from_anvl(ezid_anvl)
    add_to_db(ark_obj)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("username", help="EZID username.")
    parser.add_argument("password", help="EZID password.")
    parser.add_argument(
        "action",
        help="""Action to take. Accepted arguments are: 'anvl', 'batch',
'delete', 'ead', 'update', and 'view'. 'anvl' mints new ARKs from ANVL
metadata and 'ead' mints ARKs from EAD XML.""",
    )
    parser.add_argument(
        "ark",
        help="""After 'anvl' or 'ead', this must be an ARK shoulder. After
'delete', 'update', or 'view', it must be an ARK. After 'batch' it may be any
character or characters; a hyphen is recommended.""",
    )
    parser.add_argument(
        "--anvl",
        help="A string of one or more key-value pairs separated by a newline character.",
    )
    parser.add_argument(
        "--anvl-in",
        help="Input file consisting of key-value pairs seperated by newline characters.",
    )
    parser.add_argument(
        "--anvl-out", help="Output file for generated ANVL-formatted metadata."
    )
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
        "--output-file", help="Output file for recording EZID API response."
    )
    parser.add_argument(
        "--sources", nargs="+", help="Metadata sources to mint ARKs from."
    )

    args = parser.parse_args()

    if not db_exists():
        create_db(engine)

    if args.action == "ead":
        ead_xml = get_sources(args.sources)

        if args.anvl_out:
            anvls = generate_anvl_fields_from_ead_xml(ead_xml, args.anvl_out)
        else:
            anvls = generate_anvl_fields_from_ead_xml(ead_xml)

        for anvl in anvls:
            submit_md(args)
    elif args.action == "anvl":
        submit_md(args)
    elif args.action == "batch":
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
        view_anvl(args.username, args.password, args.ark)


if __name__ == "__main__":
    main()
