import requests
import json

SUMMARY_INDEX = "daily_totals"
ENDPOINT = "http://localhost:9200"
HEADERS = {'Content-Type': 'application/json'}

test_query = {
    "size": 1,
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

response_json = requests.get(f"{ENDPOINT}/{SUMMARY_INDEX}/_search", data=json.dumps(test_query), headers=HEADERS).json()

response_json