"""
script that calls api.govinfo.gov and downloads U.S. Laws, creating a document database of laws.

In particular, it downloads the Public Laws (PLAW), which can be thought of as the final stage of the law lifecycle:
Bill (proposal) -> Statute (passed by Congress and President) -> Public Law (enacted) ----> U.S. Code (codified every 6 years)

it works by grabbing data from the bulk data endpoints: 

it rate limits the API calls to 1000 per rolling hour, which is the limit of the API: https://api.data.gov/docs/developer-manual/#web-service-rate-limits
"""
from ratelimit import limits, sleep_and_retry
import requests
from datetime import timedelta
import dotenv
import os
import json
import xml.etree.ElementTree as ET

import logging

logger = logging.getLogger(__name__)
# save the logs to a file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s",
    handlers=[
        logging.FileHandler("logs/get_data.log"),
        logging.StreamHandler(),
    ],
)

dotenv.load_dotenv()
DATAGOV_API_KEY = os.getenv("DATAGOV_API_KEY")
MAX_PAGE_SIZE = 1000
# bulk endpoint for public laws
bulk_data_endpoint = "https://www.govinfo.gov/bulkdata/json/PLAW/{congress}/public"
bulk_data_dir = "data/raw/bulk/"
individual_law_data_dir = "data/raw/individual/"


@sleep_and_retry
@limits(calls=1000, period=timedelta(hours=1).total_seconds())  # 1000 calls per hour
def call_api(url):
    # ensure set the appropriate headers for json, e.g. Accept: application/json
    return requests.get(url, headers={"Accept": "application/json"})


def save_public_laws(congress, response):
    # create the directory if it doesn't exist
    if not os.path.exists(bulk_data_dir):
        os.makedirs(bulk_data_dir)

    # save the response to a file
    with open(f"{bulk_data_dir}plaw_{congress}.json", "w") as f:
        f.write(response.text)


def check_if_raw_data_exists(congress):
    return os.path.exists(f"{bulk_data_dir}plaw_{congress}.json")


def get_bulk_public_laws():
    """
    Iterate through the bulk data endpoint for public laws and download the data.
    """
    congresses = range(113, 119)  # 113th to 118th congresses

    for congress in congresses:
        # check if the raw data exists for this congress, and if so, skip it
        exists = check_if_raw_data_exists(congress)
        if exists:
            logger.info(f"Skipping congress {congress}, already exists")
            continue
        logger.info(f"Starting congress {congress}")
        endpoint = bulk_data_endpoint.format(congress=congress)
        response = call_api(endpoint)
        # if the response is 200, then we have data and are not finished
        if response.status_code == 200:
            save_public_laws(congress, response)
        else:
            raise Exception(f"Response code {response.status_code} for {endpoint}")


def process_bulk_plaw_file(bulk_plaw_file):
    """parse the bulk data json, extract urls for individual laws, and download the individual laws"""
    # create the directory if it doesn't exist
    if not os.path.exists(individual_law_data_dir):
        os.makedirs(individual_law_data_dir)

    congress = bulk_plaw_file.split("_")[1].split(".")[0]
    logger.info(f"Processing bulk_plaw_file {bulk_plaw_file} for congress {congress}")

    # load the bulk data json
    with open(f"{bulk_data_dir}{bulk_plaw_file}", "r") as f:
        bulk_data = json.load(f)

    # bulk_data has only 1 key for "files"
    bulk_data = bulk_data["files"]

    # iterate through the bulk data json and extract the urls for the individual laws. the url are under the "link" key
    for law_info in bulk_data:
        name = law_info["name"]
        if os.path.exists(f"{individual_law_data_dir}{name}"):
            logger.info(f"Skipping {name}, already exists")
            continue
        xml_url = law_info["link"]
        if not xml_url.endswith(".xml"):
            logger.info(f"Skipping {name}, not an xml file")
            continue

        logger.info(f"Downloading {name} from {xml_url}")
        # download XML file
        response = call_api(xml_url)
        if response.status_code == 200:
            # save the response to a file
            with open(f"{individual_law_data_dir}{name}", "w") as f:
                f.write(response.text)
        else:
            raise Exception(f"Response code {response.status_code} for {xml_url}")


def get_individual_laws():
    """process all the bulk data files and save the individual laws"""
    # scan the bulk data directory for json files
    bulk_plaw_files = os.listdir(bulk_data_dir)
    for bulk_plaw_file in bulk_plaw_files:
        process_bulk_plaw_file(bulk_plaw_file)


def process_individual_law_file(individual_law_file):
    """parse the individual law xml and save as a txt file"""
    try:
        tree = ET.parse(f"{individual_law_data_dir}{individual_law_file}")
    except ET.ParseError as e:
        logger.error(f"Error parsing {individual_law_file}: {e}. \n deleting file")
        os.remove(f"{individual_law_data_dir}{individual_law_file}")
        return

    root = tree.getroot()

    ignore_sections = [
        "actionDescription",
        "toc",
        "designator",
        "label",
        "referenceItem",
        "num",
        "citableAs",
        "approvedDate",
        "publisher",
        "creator",
        "format",
        "language",
        "rights",
        "congress",
        "docNumber",
        "ref",
        "committee",
        "type",
        "language",
        "format",
        "page",
        "processedBy",
        "processedDate",
        "publicPrivate",
        "endMarker",
    ]

    ignore_sections_prefixed = set()
    for prefix in [
        "{http://schemas.gpo.gov/xml/uslm}",
        "{http://purl.org/dc/elements/1.1/}",
        "{http://purl.org/dc/terms/}",
    ]:
        for section in ignore_sections:
            ignore_sections_prefixed.add(f"{prefix}{section}")

    # 2. inner texts
    # get all the text from the xml
    inner_texts = []
    for element in root.iter():
        if element.tag in ignore_sections_prefixed:
            # print(
            #     f"Skipping tag `{element.tag}`, text `{element.text}`, tail `{element.tail}`"
            # )
            continue
        else:
            if element.text:
                if element.text.strip() == "":
                    continue
                inner_texts.append(element.text)
                # fixme: we are sometimes dropping important content in element.tail

    # look for a item called "LEGISLATIVE HISTORY", and if it is found, delete it and all texts after it
    legislative_history_index = None
    for i, text in enumerate(inner_texts):
        if text.upper() == "LEGISLATIVE HISTORY":
            legislative_history_index = i
            break
    if legislative_history_index:
        inner_texts = inner_texts[:legislative_history_index]

    text = "\n".join(inner_texts)

    # 3. save the text to a file
    os.makedirs("data/processed", exist_ok=True)
    with open(
        f"data/processed/{individual_law_file}".replace(".xml", ".txt"), "w"
    ) as f:
        f.write(text)


def process_individual_laws():
    """process all the individual laws and save the data to the database"""
    # scan the individual law data directory for xml files
    individual_law_files = os.listdir(individual_law_data_dir)
    for individual_law_file in individual_law_files:
        process_individual_law_file(individual_law_file)


if __name__ == "__main__":
    logger.info("Starting get_data.py")
    get_bulk_public_laws()
    get_individual_laws()
    process_individual_laws()
