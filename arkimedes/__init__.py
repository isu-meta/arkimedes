import requests

CONCAT_STRING = "\n---\n"


def get_from_files(fs):
    files = []
    for f in fs:
        with open(f, "r", encoding="utf-8") as fh:
            content = fh.read(content)
        files.append(content)

    return files


def get_from_urls(urls):
    files = []
    for url in urls:
        try:
            request = requests.get(url)
            if request.ok:
                files.append(request.content)
        except:
            print(f"Item not found: {url}")

    return files


def get_sources(sources):
    fs = [s for s in sources if not s.startswith("http")]
    urls = [s for s in sources if s.startswith("http")]

    files = get_from_files(fs)
    files.extend(get_from_urls(urls))

    return files


def convert_date_string_to_iso(date_string, date_format="MMDDYYYY", delimiter="/"):
    date_bits = date_string.split(delimiter)

    if date_format == "MMDDYYYY":
        date_out = "-".join(
            [date_bits[2], date_bits[0].zfill(2), date_bits[1].zfill(2)]
        )

    return date_out
