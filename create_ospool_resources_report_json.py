#!/usr/bin/env python3

import requests
import json
import datetime
import os
from verification import verify_latest_report
from util import write_document_to_file

DATA_DIRECTORY = "data"
SUMMARY_INDEX = "osg-schedd-*"
ENDPOINT = "http://localhost:9200"


def get_ospool_resources_report_json():

    query = {
        "size": 1000,
        "aggs": {
            "projects": {
                "terms": {
                    "field": "ProjectName.keyword",
                    "size": 1000,
                }
            }
        },
        "query": {
            "bool": {
                "filter": [
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
                                "ap20.uc.osg-htc.org",
                                "ap21.uc.osg-htc.org",
                                "ap22.uc.osg-htc.org",
                                "ap23.uc.osg-htc.org",
                                "ap40.uw.osg-htc.org"
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
    project_names = [i['key'] for i in response_json['aggregations']['resources']['buckets']]

    return project_names


if __name__ == "__main__":

    active_ospool_resources = get_ospool_resources_report_json()
    write_document_to_file(active_ospool_resources, DATA_DIRECTORY, f"ospool_projects.json", True)