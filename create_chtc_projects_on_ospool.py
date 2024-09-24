#!/usr/bin/env python3

import requests
import json

from util import write_document_to_file

SUMMARY_INDEX = "chtc-schedd-*"
ENDPOINT = "http://localhost:9200"
DATA_DIRECTORY = "data"

def get_chtc_projects_on_ospool():

    query = {
        "size": 0,
        "aggs": {
            "projects": {
                "terms": {
                    "field": "ProjectName.keyword",
                    "size": 10000,
                }
            }
        },
        "query": {
            "bool": {
                "filter": [{
                    "terms": {"LastRemotePool.keyword": ['cm-1.ospool.osg-htc.org', "cm-2.ospool.osg-htc.org", "flock.opensciencegrid.org"]},
                }],
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

    chtc_projects_on_ospool = [v['key'] for v in response_json['aggregations']['projects']['buckets']]

    return chtc_projects_on_ospool

if __name__ == "__main__":

    chtc_projects_on_ospool = get_chtc_projects_on_ospool()

    write_document_to_file(chtc_projects_on_ospool, DATA_DIRECTORY, f"chtc_projects_running_on_ospool.json", True)