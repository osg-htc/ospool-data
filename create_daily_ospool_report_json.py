#!/usr/bin/env python3

import requests
import json
import os
from verification import verify_latest_report

DATA_DIRECTORY = "data/daily_reports"
SUMMARY_INDEX = "daily_totals"
ENDPOINT = "http://localhost:9200"
OUTLIERS_WE_CARE_ABOUT = {"all_cpu_hours", "num_uniq_job_ids", "num_projects", "num_users", "total_files_xferd"}
EXPECTED_KEYS = [
    "num_uniq_job_ids",
    "total_files_xferd",
    "all_cpu_hours",
    "num_users",
    "num_uniq_job_ids",
    "num_institutions"
]

def get_daily_reports(size):
    """Get OSPool daily reports from elastic search ordered by date"""

    query = {
        "size": size,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"query": "OSG-schedd-job-history"}},
                    {"term": {"report_period": "daily"}},
                ]
            }
        },
        "sort": [
            {"date": "desc"}
        ]
    }

    # Pull out the document and dump it in a dated file
    response = requests.get(
        f"{ENDPOINT}/{SUMMARY_INDEX}/_search",
        data=json.dumps(query),
        headers={'Content-Type': 'application/json'}
    )
    response_json = response.json()
    documents = response_json['hits']['hits']

    return documents


def write_document_to_file(document: dict, latest: bool = False, overwrite: bool = False):

    output_path = f"{DATA_DIRECTORY}/{document['date']}.json" if not latest else f"{DATA_DIRECTORY}/latest.json"

    if overwrite or not os.path.isfile(output_path):
        with open(output_path, "w") as fp:
            json.dump(document, fp)


if __name__ == "__main__":

    documents = get_daily_reports(9999)

    for document in documents:
        write_document_to_file(document["_source"])

    if verify_latest_report(OUTLIERS_WE_CARE_ABOUT, EXPECTED_KEYS):

        latest_document = documents[0]
        write_document_to_file(latest_document["_source"], True, True)

    else:

        print("Found some outliers or missing keys, check latest report.")
        print(documents[0]["_source"]["date"])
        exit(1)

