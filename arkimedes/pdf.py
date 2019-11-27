from io import BytesIO, StringIO

from PyPDF2 import PdfFileReader

from arkimedes import convert_date_string_to_iso
from arkimedes.ezid import build_anvl, generate_anvl_strings
from arkimedes.qa import check_lc_naf


def _generate_anvl_from_conservation_report(pdf_data):
    pdf, pdf_url = pdf_data
    pdf_content = StringIO()

    reader = PdfFileReader(BytesIO(pdf))

    for page in reader.pages:
        pdf_content.write(page.extractText())

    text = pdf_content.getvalue()

    creator_start_index = text.index("Conservator")
    creator_end_index = text.index("Call Number")
    creator = check_lc_naf(
        text[creator_start_index:creator_end_index].split(":")[1].strip()
    )

    title_start_index = text.index("Title")
    title_end_index = text.index("Collection")
    title = text[title_start_index:title_end_index].split(":")[1].strip()

    date_start_index = text.index("Date of report")
    date_end_index = text.index("Conservator")
    date = convert_date_string_to_iso(
        text[date_start_index:date_end_index].split(":")[1].strip()
    )

    return build_anvl(creator, title, date, pdf_url)


def generate_anvl_from_conservation_reports(pdf_data, output_file=None):
    return generate_anvl_strings(
        (pdf_data, _generate_anvl_from_conservation_report, output_file)
    )
