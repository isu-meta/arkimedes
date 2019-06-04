import requests

CONCAT_STRING = "\n---\n"


def get_xml_from_urls(urls):
    xml_files = []
    for url in urls:
        try:
            request = requests.get(url)
            if request.ok:
                xml_files.append(request.content)
        except:
            print(f"Finding aid not published: {url}")

    return xml_files
