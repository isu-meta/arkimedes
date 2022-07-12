from inspect import cleandoc
import sys
from time import sleep

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


def upload_anvl(
    user_name, password, shoulder, anvl_text, action="mint", output_file=None
):
    base_url = "https://ezid.cdlib.org"
    headers = {"Content-Type": "text/plain; charset=UTF-8"}

    if action == "mint":
        request_url = "/".join([base_url, "shoulder", shoulder])
    elif action == "update":
        request_url = "/".join([base_url, "id", shoulder])

    r = requests.post(
        request_url, headers=headers, data=anvl_text.encode("utf-8"), auth=(user_name, password)
    )
    ark = r.text[9:]

    print(anvl_text)
    print(r.text)

    if output_file is not None:
        with open(output_file, "a", encoding="utf-8") as fh:
            fh.write(f":: {ark}\n")
            fh.write(f"{anvl_text.strip()}\n")

    return ark


def view_anvl(user_name, password, ark, print_=True):
    base_url = "https://ezid.cdlib.org/id"
    request_url = "/".join([base_url, ark])
    r = requests.get(request_url, auth=(user_name, password))

    if print_:
        print(r.text)

    return r.text
