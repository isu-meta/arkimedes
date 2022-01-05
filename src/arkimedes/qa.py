from urllib.parse import quote_plus

from fuzzywuzzy import fuzz
from lxml import etree, html
import requests


def check_lc_naf(name):
    return_name = name
    request_string = f"http://id.loc.gov/search/?q={quote_plus(name)}&q=cs%3ahttp%3a%2f%2fid.loc.gov%2fauthorities%2fnames"
    request = requests.get(request_string)

    if request.ok:
        # collect results from page (let's assume that results on additional
        # pages can't possibly be useful)
        results_xpath = "//tbody[@class='tbody-group']"
        name_xpath = "tr/td/a[@title='Click to view record']"
        matches = []

        min_threshold = 60
        threshold = 70

        results_page = html.fromstring(request.text)

        results = results_page.xpath(results_xpath)

        # see if there are any results
        if results:
            for result in results:
                result_name = result.xpath(name_xpath)[0].text
                ratio = fuzz.ratio(name, result_name)
                if ratio > min_threshold:
                    matches.append((ratio, name, result_name))

        if len(matches) > 1:
            matches.sort(reverse=True)
            one, two = matches[0], matches[1]
            if one[0] == two[0]:
                print(
                    f"Uncertain match. Original: {name}. Top 2 matches: {one} | {two}.\n"
                )
            elif one[0] < threshold:
                print(f"Uncertain match: Original: {name}.  Best match: {one}\n")
            else:
                return_name = matches[0][2]
    else:
        print(f"Could not retrieve {request_string}.")

    return return_name
