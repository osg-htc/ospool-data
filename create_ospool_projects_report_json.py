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
    project_names = [i['key'] for i in response_json['aggregations']['projects']['buckets']]

    return project_names


def verify_ospool_projects(new_projects):
    current_projects = None
    with open(f"{DATA_DIRECTORY}/ospool_projects.json", "r") as fp:
        current_projects = json.load(fp)

    current_projects = set(current_projects)
    new_projects = set(new_projects)

    if not new_projects.issuperset(current_projects):
        print(f"Projects Missing in New Projects{current_projects.difference(new_projects)}")

    return new_projects.issuperset(current_projects)


if __name__ == "__main__":

    ospool_projects = get_ospool_resources_report_json()

    if verify_ospool_projects(ospool_projects):
        write_document_to_file(ospool_projects, DATA_DIRECTORY, f"ospool_projects.json", True)
    else:
        print("New Projects are not a superset of the previous projects!")
