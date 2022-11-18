#!/usr/bin/env python3

import requests
import json
import os

DATA_DIRECTORY = "data/daily_reports"
SUMMARY_INDEX = "daily_totals"
ENDPOINT = "http://localhost:9200"


def get_daily_reports(size):
    """Get OSPool daily reports from elastic search ordered by date"""
    query = {
        "size": size,
        "query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "query": "OSG-schedd-job-history"
                        }
                    },
                    {
                        "term": {
                            "report_period": "daily"
                        }
                    },
                ]
            }
        },
        "sort": [
            {
                "date": "desc"
            }
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


documents = get_daily_reports(9999)

# Write the most recent file to latest
with open(f"{DATA_DIRECTORY}/latest.json", "w") as fp:
    json.dump(documents[0]["_source"], fp)

# Write the rest to a dated file
for document in documents:
    json_output_path = f"{DATA_DIRECTORY}/{document['_source']['date']}.json"

    # No need to write to an existing file
    if not os.path.isfile(json_output_path):

        with open(json_output_path, "w") as fp:
            json.dump(document["_source"], fp)
