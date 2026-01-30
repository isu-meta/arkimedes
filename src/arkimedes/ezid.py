import csv
from itertools import zip_longest
import sys
from time import sleep

from lxml import etree
import requests


def anvls_to_dict(anvls):
    """Convert a multi-record ANVL string to a list of dictionaries.

    Parameters:
    -----------
    anvl : str
        An ANVL-formatted string.

    Returns:
    --------
    list[dict]
    """
    return [anvl_to_dict(a) for a in anvls.strip().split("\n\n")]


def anvl_to_dict(anvl):
    """Convert single-record ANVL string to dictionary.

    Parameters:
    -----------
    anvl : str
        An ANVL-formatted string.

    Returns:
    --------
    dict
    """
    ark_dict = {}
    for line in anvl.split("\n"):
        if line.startswith("::"):
            ark_dict["ark"] = line[3:]
        elif line.strip() != "":
            # limit the split to only the first colon to not mangle URIs
            k, v = line.split(":", 1)
            ark_dict[k.strip()] = v.strip()

    return ark_dict


def batch_download(
    username, password, format_="anvl", compression="zip", args=""
):
    """Batch download ARKs from EZID.

    Parameters:
    -----------
    username : str
        EZID username.
    password : str
        EZID password.
    format_ : str
        Valid inputs are 'anvl', 'csv', or 'xml'. Defaults to 'anvl'.
        CSV requests require
    compression : str
        Valid inputs are 'gzip' or 'zip'. Defaults to 'zip'.
    args : str
        String to pass to EZID API. Strings should follow the
        format '&key=value&key=value...'. A full list of parameters
        can be found here:
        https://ezid.cdlib.org/doc/apidoc.html#parameters

    Returns
    -------
    None
    """
    url = "https://ezid.cdlib.org/download_request"
    headers = {"Content-type": "application/x-www-form-urlencoded"}
    query = f"format={format_}&compression={compression}{args}"

    r = requests.post(
        url, data=query, auth=(username, password), headers=headers
    )

    if not r.ok or not r.text.startswith("success: "):
        print(
            f"Request failure!\n----------------\n{r.status_code}: {r.text}\n"
        )

        if format_ == "csv":
            print("""CSV requests must include headers for the columns you're
requesting. They may be passed to arkimedes with the '---batch-args' flag
and must be formatted like so: '&column=dc.title&column=dc.creator...',
replacing the column names with the headers you want. For a full overview
of possible headers, please refer to EZID's documentation:
https://ezid.cdlib.org/doc/apidoc.html#download-formats
""")

        sys.exit(1)

    url = r.text[9:]
    file_name = url[url.rfind("/") + 1 :]

    print(f"Acquired download URL: {url}")
    print("It may take EZID a few minutes to prepare the file.")
    print("Waiting on EZID..", end="")
    r = requests.get(url)
    sleep_count = 0
    # It up to 3 minutes for EZID to prepare a batch download.
    # Check every 5 seconds for 5 minutes before giving up.
    while sleep_count < 60:
        print(".", end="", flush=True)
        r = requests.get(url)
        if r.status_code == 200:
            with open(file_name, "wb") as fh:
                fh.write(r.content)
            break

        sleep(5)
        sleep_count += 1

    if r.status_code == 200:
        print(file_name)
    else:
        print(f"Download failed.\nTry downloading manually from: {url}")


def build_anvl(anvl_dict):
    """Convert a dictionary to an ANVL string.

    Parameters
    ----------
    anvl_dict : dict[str, str]
        Dictionary to convert.

    Returns
    -------
    str
    """
    lines = [": ".join((k, anvl_dict[k])) for k in anvl_dict.keys()]
    return "\n".join(lines)


def convert_anvl_file_to_tsv(anvl_file, tsv_file):
    """Convert an ANVL file to a TSV.

    Parameters
    ----------
    anvl_file : str or Path
    tsv_file : str or Path

    Returns
    -------
    None
    """
    anvls = load_anvl_as_dict(anvl_file)
    keys = set()
    for anvl in anvls:
        keys.update(*anvl.keys())
    key_list = list(keys)
    with open(tsv_file, "w", encoding="utf-8") as fh:
        for anvl in anvls:
            line = []
            for k in key_list:
                line.append(anvl.get(k, ""))
            fh.write("\t".join(line))
            fh.write("\n")


def find_reusable(username, password):
    """Search for ARK with 'reuse' in the title.

    Parameters
    ----------
    username : str
        Admin username for EZID.
    password : str
        Admin user password for EZID.

    Returns
    -------
    Iterator[dict[str, str]]
        Yields a dictionary with the fields "title", "creator", "ark",
        "owner", "create_time", "update_time", and "id_status"
    """
    results = query(title="reuse", username=username, password=password)
    return results


def find_url(url, username, password):
    """Search for ARK by URL.

    Parameters
    ----------
    url : str
        URL to search for.
    username : str
        Admin username for EZID.
    password : str
        Admin user password for EZID.

    Returns
    -------
    Iterator[dict[str, str]]
        Yields a dictionary with the fields "title", "creator", "ark",
        "owner", "create_time", "update_time", and "id_status"
    """
    results = query(target=url, username=username, password=password)
    return results


def generate_anvl_strings(data_source, parser, output_file=None):
    """Generate anvl strings.

    Parameters
    ----------
    data_source : Iterable or Iterator
        The objects to be parsed.
    parser : Callable
        The function to extract ANVL strings from data_source.
    output_file : None or str or Path
        The file to save the ANVL strings to.

    Returns
    -------
    list[str]
    """
    anvls = []

    for d in data_source:
        anvls.append(parser(d))

    if output_file is not None:
        with open(output_file, "w", encoding="utf-8") as fh:
            for a in anvls:
                fh.write(f"{a}\n")

    return anvls


def get_value_from_anvl_string(field, anvl):
    """Gets the value from an ANVL record for a given key.

    Parameters
    ----------
    field : str
        Key to get value from.
    anvl : str
        Record to get value from.

    Returns
    -------
    str or None
    """
    anvl_dict = anvl_to_dict(anvl)

    return anvl_dict.get(field)


def load_anvl_as_dict(anvl_file):
    """Creates a generator that yields ARK records as dictionaries.

    Parameters
    ----------
    anvl_file : str or pathlib.Path
        File to load ARK records from.

    Returns
    -------
    generator
        Yields a dictionary where each key, value pair corresponds to an
        ANVL record field.
    """
    ark_dict = {}
    with open(anvl_file, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line == "":
                yield ark_dict
            elif line.startswith("::"):
                ark_dict["ark"] = line[3:]
            else:
                k, v = line.split(":", 1)
                ark_dict[k] = v.strip()


def load_anvl_as_str(anvl_file):
    """Returns a list of ARK records as ANVL strings.

    Parameters
    ----------
    anvl_file : str or pathlib.Path
        File to load ARK records from.

    Returns
    -------
    list
        List of ARK records as a string.
    """

    with open(anvl_file, "r", encoding="utf-8") as fh:
        return list(fh.read().split("\n\n"))


def load_anvl_as_str_from_tsv(tsv_file):
    """Creates a generator that yields ANVL strings from a TSV.

    Requires a tab-separated-values file with the following
    headers: dc.creator, dc.title, dc.date, _target, and dc.type.

    Parameters
    ----------
    tsv_file : str or pathlib.Path
        File to load metadata from.

    Returns
    -------
    generator
        Yields an ANVL record as a string.
    """
    with open(tsv_file, "r", encoding="utf-8") as fh:
        for row in load_anvl_as_dict_from_tsv(tsv_file):
            yield build_anvl(row)


def load_anvl_as_dict_from_tsv(tsv_file):
    """Creates a generator that yields ANVL dictionaries from a TSV.

    Requires a tab-separated-values file with the following
    headers: dc.creator, dc.title, dc.date, _target, and dc.type.

    Parameters
    ----------
    tsv_file : str or pathlib.Path
        File to load metadata from.

    Returns
    -------
    generator
        Yields an ANVL record as a dictionary.
    """
    with open(tsv_file, "r", encoding="utf-8") as fh:
        dialect = csv.Sniffer().sniff(fh.read(1024))
        fh.seek(0)
        reader = csv.DictReader(fh, dialect=dialect)
        for row in reader:
            yield row


def login(username, password):
    """Login to the EZID website.

    Parameters
    ----------
    username : str
    password : str

    Returns
    -------
    requests.Session() object
    """
    s = requests.Session()
    # Need to spoof the User-Agent to avoid getting a 405 error
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
    }
    payload = {
        "next": "/",
        "username": username,
        "password": password,
    }
    s.post("https://ezid.cdlib.org/login", data=payload, headers=headers)
    return s


def query(
    *,
    ps=1000,
    order_by="c_update_time",
    sort_="asc",
    owner_selected="user_iastate_lib",
    c_title="t",
    c_creator="t",
    c_identifier="t",
    c_owner="t",
    c_create_time="t",
    c_update_time="t",
    c_id_status="t",
    keywords="",
    identifier="",
    title="",
    creator="",
    publisher="",
    pubyear_from="",
    pubyear_to="",
    object_type="",
    target="",
    id_type="",
    create_time_from="",
    create_time_to="",
    update_time_from="",
    update_time_to="",
    id_status="",
    p=1,
    username,
    password,
):
    """Search for ARKs by metadata.

    Since the EZID API lacks any search functionality beyond
    retrieving metadata for specific ARKs (for some horrible reason,)
    this function searches the via the manage interface on the
    EZID website.

    The search returns at most the number of records specified in `ps`.
    There is currently no functionality to verify whether or not there
    are more matching records than specified in `ps` nor to return
    all matching records if the number of matches is higher than `ps`.

    Parameters
    ----------
    ps : int
        Defaults to 1000
    order_by : str
        Defaults to "c_update_time"
    sort_ : str
        Defaults to "asc"
    owner_selected : str
        Defaults to "t"
    c_title : str
        Defaults to "t"
    c_creator : str
        Defaults to "t"
    c_identifier : str
        Defaults to "t"
    c_owner : str
        Defaults to "t"
    c_create_time : str
        Defaults to "t"
    c_update_time : str
        Defaults to "t"
    c_id_status : str
        Defaults to "t"
    keywords : str
        Defaults to ""
    identifier : str
        Defaults to ""
    title : str
        Defaults to ""
    creator : str
        Defaults to ""
    publisher : str
        Defaults to ""
    pubyear_from : str
        Defaults to ""
    pubyear_to : str
        Defaults to ""
    object_type : str
        Defaults to ""
    target : str
        Defaults to ""
    id_type : str
        Defaults to ""
    create_time_from : str
        Defaults to ""
    create_time_to : str
        Defaults to ""
    update_time_from : str
        Defaults to ""
    update_time_to : str
        Defaults to ""
    id_status : str
        Defaults to ""
    p : int
        Defaults to 1
    username : str
    password : str

    Returns
    -------
    Iterator[dict[str, str]]
        Yields a dictionary with the fields "title", "creator", "ark",
        "owner", "create_time", "update_time", and "id_status"
    """
    url = "https://ezid.cdlib.org/manage"
    query = {
        "ps": ps,
        "order_by": order_by,
        "sort": sort_,
        "owner_selected": owner_selected,
        "c_title": c_title,
        "c_creator": c_creator,
        "c_identifier": c_identifier,
        "c_owner": c_owner,
        "c_create_time": c_create_time,
        "c_update_time": c_update_time,
        "c_id_status": c_id_status,
        "keywords": keywords,
        "identifier": identifier,
        "title": title,
        "creator": creator,
        "publisher": publisher,
        "pubyear_from": pubyear_from,
        "pubyear_to": pubyear_to,
        "object_type": object_type,
        "target": target,
        "id_type": id_type,
        "create_time_from": create_time_from,
        "create_time_to": create_time_to,
        "update_time_from": update_time_from,
        "update_time_to": update_time_to,
        "id_status": id_status,
        "p": p,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
    }

    titles = []
    creators = []
    identifiers = []
    owners = []
    create_times = []
    update_times = []
    id_statuses = []

    s = login(username, password)
    titles_xpath = "//td[@class='c_title']/text()"
    creators_xpath = "//td[@class='c_creator']/text()"
    identifiers_xpath = "//td[@class='c_identifier']/a/text()"
    owners_xpath = "//td[@class='c_owner']/text()"
    create_times_xpath = "//td[@class='c_create_time']/text()"
    update_times_xpath = "//td[@class='c_update_time']/text()"
    id_statuses_xpath = "//td[@class='c_id_status']/text()"

    def get_all_results():
        r = s.get(url, params=query, headers=headers)
        tree = etree.HTML(r.text)

        rp = tree.xpath(
            "string(//input[@id='page-directselect-bottom']/@max/text())"
        )

        titles.extend(tree.xpath(titles_xpath))
        creators.extend(tree.xpath(creators_xpath))
        identifiers.extend(tree.xpath(identifiers_xpath))
        owners.extend(tree.xpath(owners_xpath))
        create_times.extend(tree.xpath(create_times_xpath))
        update_times.extend(tree.xpath(update_times_xpath))
        id_statuses.extend(tree.xpath(id_statuses_xpath))

        return rp

    result_pages = get_all_results()

    if result_pages:
        max_pages = int(result_pages)
        p = 2

        while p <= max_pages:
            query["p"] = p
            get_all_results()
            p += 1

    results = (
        dict(
            zip(
                (
                    "creator",
                    "title",
                    "ark",
                    "owner",
                    "created",
                    "updated",
                    "id_status",
                ),
                row,
            )
        )
        for row in zip_longest(
            creators,
            titles,
            identifiers,
            owners,
            create_times,
            update_times,
            id_statuses,
            fillvalue="",
        )
    )

    return results


def upload_anvl(
    username, password, shoulder, anvl_text, action="mint", output_file=None
):
    """Mint or update ARKs by uploading a single ANVL record.

    Parameters
    ----------
    username : str
        Admin username for EZID.
    password : str
        Admin user password for EZID.
    shoulder : str
        ARK prefix.
    anvl_text : str
        ARK metadata to upload.
    action : str
       Can be 'mint' or 'update'. Whether we're minting a new ARK or
       updating an existing one.
    output_file : None or str or Path
        Optional. File to save ARK ID and metadata to.

    Returns
    -------
    str
       The ARK for the created or updated record.
    """
    base_url = "https://ezid.cdlib.org"
    headers = {"Content-Type": "text/plain; charset=UTF-8"}

    if action == "mint":
        request_url = "/".join([base_url, "shoulder", shoulder])
    elif action == "update":
        request_url = "/".join([base_url, "id", shoulder])
    else:
        raise ValueError(f"'{action}' is invalid. Must be 'upload' or 'mint'.")

    r = requests.post(
        request_url,
        headers=headers,
        data=anvl_text.encode("utf-8"),
        auth=(username, password),
    )
    ark = r.text[9:]

    print(anvl_text)
    print(r.text)

    if output_file is not None:
        with open(output_file, "a", encoding="utf-8") as fh:
            fh.write(f":: {ark}\n")
            fh.write(f"{anvl_text.strip()}\n")

    return ark


def view_anvl(username, password, ark, print_=True):
    """Get the metadata for a given ARK.

    Parameters
    ----------
    username : str
        Admin username for EZID.
    password : str
        Admin user password for EZID.
    ark : str
        The ARK that we want to view the metadata of.
    print_ : bool
       Whether or not to print the metadata to standard output.

    Returns
    -------
    str
        ARK metadata.
    """
    base_url = "https://ezid.cdlib.org/id"
    request_url = "/".join([base_url, ark])
    r = requests.get(request_url, auth=(username, password))

    if print_:
        print(r.text)

    return r.text
