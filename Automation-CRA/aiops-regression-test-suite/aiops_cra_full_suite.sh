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
# modules_list="funct_summary_view or funct_detailed_view_filter or \
#               funct_analytics_view_filter or funct_heat_map_cie"
modules_list="funct_summary_view or \
              funct_analytics_view_filter "
report_file="report_full/aiops_regression_cra_$name.html"
email_recipient="arjunraja@ibm.com"
pytest -m "$modules_list" --html=$report_file --self-contained-html -v

# echo "##### uploading the log to git #####"
# if [ -f "$report_file" ]; then
#   git config --global user.name "Arjun R"
#   git config --global user.email "arjunraja@ibm.com" # replace with actual email
#   git add "$report_file"
#   git commit -m 'add regression report'
#   git push origin main
#   echo "Report file found and pushed to GitHub."
# else
#   echo "Report file not found."
# fi
# to delete the virtual environment
#deactivate
#rm -rf aiops_myenv
