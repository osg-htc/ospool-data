#!/usr/bin/env bash

cd /home/clock/ospool-data

set -a
. ./secrets/gh_token
set +a

source venv/bin/activate
pip3 install -r requirements.txt &> /dev/null 2>&1

printf "----------------------------------------------------------\nRunning create_chtc_projects_on_ospool\n"
python3 create_chtc_projects_on_ospool.py

printf "----------------------------------------------------------\nRunning create_daily_ospool_report_json\n"
python3 create_daily_ospool_report_json.py

printf "----------------------------------------------------------\nRunning create_active_facilities_report_json\n"
python3 create_active_facilities_report_json.py

printf "----------------------------------------------------------\nRunning create_ospool_projects_report_json\n"
python3 create_ospool_projects_report_json.py

printf "----------------------------------------------------------\nRunning create_ospool_resources_report_json\n"
python3 create_ospool_resources_report_json.py

# Check that data was added since the last run
git diff-index --quiet HEAD
returnValue=$?

# If new data was added then push it
if [ $returnValue -ne 0 ]
then
  git fetch --all
  git merge origin/master
  find data -name '*.json' | xargs git add
  git commit -m "Update Data"
  git push https://CannonLock:$GH_TOKEN@github.com/osg-htc/ospool-data.git
fi
