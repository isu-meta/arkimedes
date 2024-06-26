"""A command-line script for managing ARKs with the arkimedes library

``arkimedes.py`` provides a command-line script for working with the
ARKimedes Python library, a library created to manage the creation
and maintenance of ARKs for the Iowa State University Library.

Examples
--------
``arkimedes.py action target [username password] [--source source_list]``

All arkimedes.py commands require an action argument and a target argument.
All commands that interact with the EZID API require an EZID username and
password to be provided.


"""
import argparse

from arkimedes import get_sources
from arkimedes.ead import generate_anvl_from_ead_xml
from arkimedes.ezid import (
    anvl_to_dict,
    batch_download,
    find_reusable,
    find_url,
    get_value_from_anvl_string,
    load_anvl_as_str,
    load_anvl_as_str_from_tsv,
    upload_anvl,
    view_anvl,
)
from arkimedes.pdf import generate_anvl_from_conservation_reports


class MissingArgumentError(Exception):
    pass


def submit_md(args, anvl, reusables=(x for x in [])):
    if args.action == "update":
        upload(args, anvl, "update")
    else:
        url = get_value_from_anvl_string("_target", anvl)

        try:
            found = next(find_url(url, args.username, args.password))
        except StopIteration:
            found = None

        if found is not None:
            print(f"An ARK has already been minted for {url}.\n")
            view_anvl(args.username, args.password, found["ark"])
            print("No new ARK minted.")
        else:
            if args.reuse:
                try:
                    reusable = next(reusables)
                    args.target = reusable["ark"]
                    upload(args, anvl, "update")
                except StopIteration:
                    upload(args, anvl, "mint")
            else:
                upload(args, anvl, "mint")


def upload(args, anvl, action):
    ark = upload_anvl(
        args.username, args.password, args.target, anvl, action, args.out
    )
    return ark


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "action",
        help="""Action to take. Accepted arguments are: 'batch-download', 'delete',
'mint-anvl', 'mint-ead', 'mint-conservation-report', 'mint-tsv', 'update', and
'view'. 'mint-anvl' mints new ARKs from ANVL metadata and 'mint-ead' mints ARKs
from EAD XML. 'mint-conservation-report' mints new ARKs from Conservation
Report PDFs. 'mint-tsv' mints new ARKs from a TSV file.""",
    )
    parser.add_argument(
        "target",
        help="""After any 'mint-' argument, this must be an ARK shoulder. After
'delete', 'update', or 'view', it must be an ARK. After 'batch-download' it may be
any character or characters; a hyphen is recommended.""",
    )
    parser.add_argument("username", nargs="?", default="", help="EZID username.")
    parser.add_argument("password", nargs="?", default="", help="EZID password.")
    parser.add_argument(
        "--batch-args",
        help="""Additional arugments to be passed to the EZID API. MUST be in
'&key=value' format. For a full list of available options, see:
https://ezid.cdlib.org/doc/apidoc.html#parameters

This argument is required if requesting a CSV from the EZID API. Pass headers to use
with CSV batch download like so: '&column=dc.creator&column=dc.title...', replacing
the column header titles with the ones you want. EZID will only return columns for
specified headers. For full details on the CSV column headers accepted by the API,
please refer to their documentation: https://ezid.cdlib.org/doc/apidoc.html#download-formats
""",
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
arkimedes will reuse an ARK record that's been marked reusable, if one
is available, rather than minting a new ARK. If no reusable record is
available, a new ARK will be minted.""",
    )

    args = parser.parse_args()

    reuseables = None
    if args.reuse:
        reusables = find_reusable(args.username, args.password)
    else:
        reusables = (x for x in [])

    if args.action == "mint-anvl":
        # Support multiple ANVL strings or files in args.source
        for anvl in args.source:
            # Check if input is an ANVL string or file
            if "\n" in anvl:
                # Support multiple records in a single ANVL string
                for a in anvl.split("\n\n"):
                    submit_md(args, a, reusables)
            else:
                # Support multiple records in a single ANVL string
                for a in load_anvl_as_str(anvl):
                    submit_md(args, a, reusables)
    elif args.action == "mint-ead":
        ead_xml = get_sources(args.source)

        anvls = generate_anvl_from_ead_xml(ead_xml)

        for anvl in anvls:
            submit_md(args, anvl, reuseables)
    elif args.action == "mint-conservation-report":
        pdfs = get_sources(args.source)
        pdf_data = zip(pdfs, args.source)

        anvls = generate_anvl_from_conservation_reports(pdf_data)

        for anvl in anvls:
            submit_md(args, anvl, reusables)
    elif args.action == "mint-tsv":
        for source in args.source:
            for anvl in load_anvl_as_str_from_tsv(source):
                submit_md(args, anvl, reusables)

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
        for anvl in args.source:
            submit_md(args, anvl)
    elif args.action == "view":
        view_anvl(args.username, args.password, args.target)


if __name__ == "__main__":
    main()
