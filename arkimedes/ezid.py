from inspect import cleandoc
import sys
from time import sleep

import requests

from arkimedes import CONCAT_STRING


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


def upload_anvl(user_name, password, shoulder, anvl_text, output_file):
    base_url = "https://ezid.cdlib.org/shoulder"
    headers = {"Content-Type": "text/plain; charset=UTF-8"}

    request_url = "/".join([base_url, shoulder])

    r = requests.post(
        request_url, headers=headers, data=anvl_text, auth=(user_name, password)
    )

    print(anvl_text)
    print(r.text)

    with open(output_file, "a", encoding="utf-8") as fh:
        fh.write(anvl_text)
        fh.write(r.text)
        fh.write(CONCAT_STRING)


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
        print(f"{r.text}\n request failed")
        sys.exit(1)

    print("waiting..", end="")

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
        print(f"download failed\nurl: {url}")
