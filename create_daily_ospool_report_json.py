#!/usr/bin/env python3

import requests
import json

DATA_DIRECTORY = "data/daily_reports"
SUMMARY_INDEX = "daily_totals"
ENDPOINT = "http://localhost:9200"
HEADERS = {'Content-Type': 'application/json'}

# Grab the most recent daily report document
query = {
    "size": 9999,
    "query": {
        "bool" : {
            "filter" : [
                { "term" : { "query": "OSG-schedd-job-history" }},
                { "term" : { "report_period": "daily" }},
            ]
        }
    },
    "sort": [
        {"date" : "desc"}
    ]
}

# Pull out the document and dump it in a dated file
response = requests.get(f"{ENDPOINT}/{SUMMARY_INDEX}/_search", data=json.dumps(query), headers=HEADERS)
response_json = response.json()
documents = response_json['hits']['hits']

for document in documents:
    json_output_path = f"{DATA_DIRECTORY}/{document['_source']['date']}.json"
    with open(json_output_path, "w") as fp:
        json.dump(document["_source"], fp)
