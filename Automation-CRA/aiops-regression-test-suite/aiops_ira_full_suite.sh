#bash
#!/bin/bash

echo "##### check .env is available #####"
if [ -f .env ]; then
  echo ".env file found, proceeding the run"
else
  echo ".env file not found, quiting the run"
  exit
fi

echo "#####" Creating virtual environment #####""
sudo apt install -y python3-venv python3-pip    #UNIX
#python3 -m pip install virtualenv    #MAC
python3 -m venv aiops_myenv
source aiops_myenv/bin/activate

echo "#####" Installing required modules #####""
pip install -r requirements.txt

name=$(date +'%Y%m%d_%H%M%S')
modules_list="funct_cbc or funct_change_request or funct_impacted_account or funct_incident_information or \
              funct_incident_summary or funct_similar_incident or funct_incident_log or funct_event_data or \
              funct_alert_timeline or funct_host_details or funct_related_incident or funct_slack_warroom or \
              funct_alert_correlation or funct_runbook or funct_feedback or funct_troubleshoot"
report_file="report_full/aiops_regression_ira_$name.html"
email_recipient="arjunraja@ibm.com"
pytest -m "$modules_list" --html=$report_file --self-contained-html -v

echo "##### uploading the log to git #####"
if [ -f "$report_file" ]; then
  git config --global user.name "Arjun R"
  git config --global user.email "arjunraja@ibm.com" # replace with actual email
  git add "$report_file"
  git commit -m 'add regression report'
  git push origin main
  echo "Report file found and pushed to GitHub."
else
  echo "Report file not found."
fi
# to delete the virtual environment
#deactivate
#rm -rf aiops_myenv
