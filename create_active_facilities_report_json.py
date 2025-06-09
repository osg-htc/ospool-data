#!/usr/bin/env python3

import requests
import json
import datetime
from util import write_document_to_file

DATA_DIRECTORY = "data/active_site_report"
SUMMARY_INDEX = "osg-schedd-*"
ENDPOINT = "http://localhost:9200"


def get_daily_active_facilities_report():

    query = {
        "size": 0,
        "query" : {
            "bool": {
                "filter": [
                    {
                        "term": {
                           "ResourceType": "Payload"
                        }
                    },
                    {
                        "range": {
                           "EndTime": {
                              "lte": int(datetime.datetime.now().timestamp()),
                              "gte": int((datetime.datetime.now() - datetime.timedelta(days=365)).timestamp())
                           }
                        }
                    },
                ]
            }
        },
        "aggs": {
            "Sites": {
                "terms": {
                    "field": "Site.keyword",
                    "size": 99999999
                }
            }
        }
    }

    # Pull out the document and dump it in a dated file
    response = requests.get(
        f"{ENDPOINT}/{SUMMARY_INDEX}/_search",
        data=json.dumps(query),
        headers={'Content-Type': 'application/json'}
    )
    response_json = response.json()
    print(response_json)
    active_facilities = [v['key'] for v in response_json['aggregations']['Sites']['buckets']]

    return active_facilities

if __name__ == "__main__":

    active_facilities = get_daily_active_facilities_report()

    write_document_to_file(active_facilities, DATA_DIRECTORY, f"latest.json", True)
