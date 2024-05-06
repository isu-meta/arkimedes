from inspect import cleandoc
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
    return [anvl_to_dict(a) for a in anvls.split("\n\n")]


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
        else:
            k, v = line.split(":", 1)
            ark_dict[k.strip()] = v.strip()

    return ark_dict


def batch_download(
    username,
    password,
    format_="anvl",
    compression="zip",
    args=""
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

    r = requests.post(url, data=query, auth=(username, password), headers=headers)

    if not r.ok or not r.text.startswith("success: "):
        print(f"Request failure!\n----------------\n{r.status_code}: {r.text}\n")

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


def build_anvl(
    creator,
    title,
    dates,
    target,
    publisher="Iowa State University Library",
    type_="Collection",
    profile="dc",
):
    return cleandoc(
        f"""erc.who:{creator}
            erc.what:{title}
            erc.when:{dates}
            dc.creator:{creator}
            dc.title:{title}
            dc.publisher:{publisher}
            dc.date:{dates}
            dc.type:{type_}
            _target:{target}
            _profile:{profile}
            """
    )


def convert_anvl_file_to_tsv(anvl_file, tsv_file):
    with open(tsv_file, "w", encoding="utf-8") as fh:
        anvls = load_anvl_as_dict(anvl_file)
        for anvl in anvls:
            fh.write("\t".join(anvl.values()))
            fh.write("\n")


def find_reusable(username, password):
    results = query(title="reuse", username=username, password=password)
    return results


def find_url(url, username, password):
    results = query(target=url, username=username, password=password)
    return results


def generate_anvl_strings(data_source, parser, output_file):
    anvls = []

    for d in data_source:
        anvls.append(parser(d))

    if output_file is not None:
        with open(output_file, "w", encoding="utf-8") as fh:
            for a in anvls:
                fh.write(f"{a}\n")

    return anvls


def get_value_from_anvl_string(field, anvl):
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
        keys = fh.readline().strip().split("\t")
        for line in fh:
            values = line.strip("\r\n").split("\t")
            md = dict(zip(keys, values))
            if "dc.publisher" not in md.keys():
                md["dc.publisher"] = "Iowa State University Library"
            anvl = build_anvl(
                md["dc.creator"],
                md["dc.title"],
                md["dc.date"],
                md["_target"],
                md["dc.publisher"],
                type_=md["dc.type"]
            )

            yield anvl


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
    for anvl in load_anvl_as_str_from_tsv(tsv_file):
        yield anvl_to_dict(anvl)


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
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"}
    payload = {"next": "/", "username": username, "password": password,}
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
    username : str
    password : str

    Returns
    -------
    generator
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
    }
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"}

    s = login(username, password)
    r = s.get(url, params=query, headers=headers)
    tree = etree.HTML(r.text)
    titles = tree.xpath("//td[@class='c_title']/a/text()")
    creators = tree.xpath("//td[@class='c_creator']/a/text()")
    identifiers = tree.xpath("//td[@class='c_identifier']/a/text()")
    owners = tree.xpath("//td[@class='c_owner']/a/text()")
    create_times = tree.xpath("//td[@class='c_create_time']/a/text()")
    update_times = tree.xpath("//td[@class='c_update_time']/a/text()")
    id_statuses = tree.xpath("//td[@class='c_id_status']/a/text()")

    print(r.text)

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
                    "id_status"
                ),
                row
            )
        )
        for row
        in zip_longest(
            creators,
            titles,
            identifiers,
            owners,
            create_times,
            update_times,
            id_statuses,
            fillvalue=""
        )
    )

    return results


def upload_anvl(
    username, password, shoulder, anvl_text, action="mint", output_file=None
):
    base_url = "https://ezid.cdlib.org"
    headers = {"Content-Type": "text/plain; charset=UTF-8"}

    if action == "mint":
        request_url = "/".join([base_url, "shoulder", shoulder])
    elif action == "update":
        request_url = "/".join([base_url, "id", shoulder])

    r = requests.post(
        request_url, headers=headers, data=anvl_text.encode("utf-8"), auth=(username, password)
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
    base_url = "https://ezid.cdlib.org/id"
    request_url = "/".join([base_url, ark])
    r = requests.get(request_url, auth=(username, password))

    if print_:
        print(r.text)

    return r.text
