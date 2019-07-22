from inspect import cleandoc
import sys
from time import sleep

import requests


def batch_download(username, password, format_="anvl", compression="zip", *args):
    """Batch download ARKs from EZID.

    Parameters:
    -----------
    username : str
        EZID username.
    password : str
        EZID password.
    format_ : str
        Valid inputs are 'anvl', 'csv', or 'xml'. Defaults to 'anvl'.
    compression : str
        Valid inputs are 'gzip' or 'zip'. Defaults to 'zip'.
    *args : str
        String(s) to pass to EZID API. Strings should follow the 
        format 'key=value'. A full list of parameters can be found here:
        https://ezid.cdlib.org/doc/apidoc.html#parameters

    Returns
    -------
    None
    """
    url = "https://ezid.cdlib.org/download_request"
    headers = {"Content-type": "application/x-www-form-urlencoded"}
    query = f"format={format_}&compression={compression}&{'&'.join(args)}"

    r = requests.post(url, data=query, auth=(username, password), headers=headers)

    if not r.ok or not r.text.startswith("success: "):
        print(f"Request failure!\n----------------\n\n{r.text}")
        sys.exit(1)

    print("Downloading ARK records from EZID..", end="")

    url = r.text[9:]
    file_name = url[url.rfind("/") + 1 :]
    r = requests.get(url)
    while True:
        print(".", end="", flush=True)
        r = requests.get(url)
        if r.status_code == 200:
            with open(file_name, "wb") as fh:
                fh.write(r.content)
            break
        else:
            sleep(5)

    if r.status_code == 200:
        print(file_name)
    else:
        print(f"Download failed\nurl: {url}")


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


def get_value_from_anvl_string(field, anvl):
    for line in anvl.split("\n"):
        if line.startswith(field):
            return line.split(":")[1].strip()


def load_arks_from_ezid_anvl(anvl_file):
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
                k, v = line.split(": ", 1)
                ark_dict[k] = v


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
        request_url, headers=headers, data=anvl_text, auth=(user_name, password)
    )
    ark = r.text[9:]

    print(anvl_text)
    print(r.text)

    if output_file is not None:
        with open(output_file, "a", encoding="utf-8") as fh:
            fh.write(f":: {ark}")
            fh.write(f"{anvl_text.strip()}\n")

    return ark


def view_anvl(user_name, password, ark, print_=True):
    base_url = "https://ezid.cdlib.org/id"
    request_url = "/".join([base_url, ark])
    r = requests.get(request_url, auth=(user_name, password))

    if print_:
        print(r.text)

    return r.text
