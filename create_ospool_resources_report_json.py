#!/usr/bin/env python3

import requests
import json
import datetime
import os
from verification import verify_latest_report
from util import write_document_to_file, get_ospool_aps, OSPOOL_COLLECTORS, OSPOOL_NON_FAIRSHARE_RESOURCES

DATA_DIRECTORY = "data/ospool_resources_report"
SUMMARY_INDEX = "osg-schedd-*"
ENDPOINT = "http://localhost:9200"


def get_ospool_resources_report_json():

    query = {
        "size": 1000,
        "runtime_mappings": {
            "ResourceName": {
                "type": "keyword",
                "script": {
                    "language": "painless",
                    "source": """
                    String res;
                    if (doc.containsKey("MachineAttrGLIDEIN_ResourceName0") && doc["MachineAttrGLIDEIN_ResourceName0.keyword"].size() > 0) {
                        res = doc["MachineAttrGLIDEIN_ResourceName0.keyword"].value;
                    } else if (doc.containsKey("MATCH_EXP_JOBGLIDEIN_ResourceName") && doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].size() > 0) {
                        res = doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].value;
                    } else {
                        res = "UNKNOWN";
                    }
                    emit(res);
                    """,
                }
            }
        },
        "aggs": {
            "resources": {
                "terms": {
                    "field": "ResourceName",
                    "missing": "UNKNOWN",
                    "size": 1024
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
                ],
                "minimum_should_match": 1,
                "should": [
                    {
                        "bool": {
                            "filter": [
                                {
                                    "terms": {
                                        "ScheddName.keyword": list(get_ospool_aps())
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
                    },
                    {
                        "terms": {
                            "LastRemotePool.keyword": list(OSPOOL_COLLECTORS)
                        }
                    },
                ],
                "must_not": [
                    {
                        "terms": {
                            "ResourceName": list(OSPOOL_NON_FAIRSHARE_RESOURCES)
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

def verify_ospool_resources(new_resources):
    with open(f"{DATA_DIRECTORY}/data/ospool_resources_report/ospool_resources.json", "r") as fp:
        current_resources = json.load(fp)

    current_resources = set(current_resources)
    new_resources = set(new_resources)

    if not new_resources.issuperset(current_resources):
        print(f"resources Missing in New resources{current_resources.difference(new_resources)}")

    return new_resources.issuperset(current_resources)


if __name__ == "__main__":

    active_ospool_resources = get_ospool_resources_report_json()

    if verify_ospool_resources(active_ospool_resources):
        clean_resources = sorted(set(active_ospool_resources))
        write_document_to_file(clean_resources, DATA_DIRECTORY, f"ospool_resources.json", True)

    else:
        print("New resources are not a superset of the previous resources!")