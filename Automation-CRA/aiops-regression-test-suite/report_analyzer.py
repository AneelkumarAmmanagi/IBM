import json
import argparse
from bs4 import BeautifulSoup

def analyze_report(html_file):
    with open(html_file, 'r') as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, 'html.parser')
    data_container = soup.find('div', {'id': 'data-container'})
    json_blob = data_container['data-jsonblob']
    data = json.loads(json_blob)
    pass_count = 0
    fail_count = 0

    for test in data['tests'].values():
        for result in test:
            if result['result'] == 'Passed':
                pass_count += 1
            elif result['result'] in ['Failed', 'Error']:
                fail_count += 1
    print(f"Pass: {pass_count}, Fail: {fail_count}, Log: {html_file}, Module: {module}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--module_list', help='Module list')
    parser.add_argument('--report_file', help='Report file')
    args = parser.parse_args()
    module = args.module_list
    report_file = args.report_file
    analyze_report(report_file)
