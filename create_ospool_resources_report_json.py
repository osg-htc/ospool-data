#!/usr/bin/env python3

import requests
import json
import datetime
import os
from verification import verify_latest_report
from util import write_document_to_file

DATA_DIRECTORY = "data/ospool_resources_report"
SUMMARY_INDEX = "osg-schedd-*"
ENDPOINT = "http://localhost:9200"


def get_ospool_resources_report_json():

    query = {
        "size": 1000,
        "aggs": {
            "resources": {
                "terms": {
                    "field": "MachineAttrGLIDEIN_ResourceName0.keyword",
                    "size": 1000,
                }
            }
        },
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "RecordTime": {
                                "gte": int((datetime.datetime.now() - datetime.timedelta(days=365)).timestamp()),
                            }
                        }
                    },
                    {
                        "term": {
                            "JobUniverse": 5,
                        }
                    },
                    {
                        "term": {
                            "JobStatus": 4,
                        }
                    },
                    {
                        "terms": {
                            "ScheddName.keyword": [
                                "login04.osgconnect.net",
                                "login05.osgconnect.net",
                            ]
                        }
                    },
                ],
                "must_not": [
                    {
                        "exists": {
                            "field": "LastRemotePool",
                        }
                    },
                ],
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
    resource_names = [i['key'] for i in response_json['aggregations']['resources']['buckets']]

    return resource_names


if __name__ == "__main__":

    active_ospool_resources = get_ospool_resources_report_json()
    write_document_to_file(active_ospool_resources, DATA_DIRECTORY, f"latest.json", True)