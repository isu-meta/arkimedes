from io import BytesIO, StringIO
import re

from pypdf import PdfReader

from arkimedes.ezid import build_anvl, generate_anvl_strings
from arkimedes.qa import check_lc_naf


def convert_date_string_to_iso(
    date_string, date_format="MMDDYYYY", delimiter="/"
):
    date_bits = date_string.split(delimiter)

    if date_format == "MMDDYYYY":
        date_out = "-".join(
            [date_bits[2], date_bits[0].zfill(2), date_bits[1].zfill(2)]
        )
    else:
        date_out = date_string

    return date_out


def _generate_anvl_from_conservation_report(pdf_data):
    pdf, pdf_url = pdf_data
    pdf_content = StringIO()

    reader = PdfReader(BytesIO(pdf))

    for page in reader.pages:
        pdf_content.write(page.extract_text())

    text = pdf_content.getvalue().replace("\n", "")

    creator_start_index = text.index("Conservator")
    creator_end_index = text.index("Call Number")

    creator = check_lc_naf(
        text[creator_start_index:creator_end_index].split(":")[1].strip()
    )

    title_start_p = re.compile(r"Title:")
    title_start_index = title_start_p.search(text).end()
    title_end_p = re.compile(r"\w+/?\w+:")
    title_end_index = (
        title_end_p.search(text[title_start_index:]).start()
        + title_start_index
    )

    title = text[title_start_index:title_end_index].strip()

    date_start_p = re.compile("Date of report:")
    date_start_index = date_start_p.search(text).end()
    date_end_p = re.compile("Conservator")
    date_end_index = (
        date_end_p.search(text[date_start_index:]).start() + date_start_index
    )
    date = text[date_start_index:date_end_index].strip()

    if date != "":
        date = convert_date_string_to_iso(date)

    return build_anvl(creator, title, date, pdf_url, type_="Text")


def generate_anvl_from_conservation_reports(pdf_data, output_file=None):
    return generate_anvl_strings(
        pdf_data, _generate_anvl_from_conservation_report, output_file
    )
