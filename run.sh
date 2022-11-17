#!/usr/bin/env bash

set -a
. ./secrets/gh_token
set +a

source venv/bin/activate
pip3 install -r requirements.txt

python create_daily_ospool_report_json.py

git fetch --all
git merge origin/master
git add .
git commit -m "Update Data"
git push https://$GH_TOKEN@github.com/osg-htc/ospool-data.git