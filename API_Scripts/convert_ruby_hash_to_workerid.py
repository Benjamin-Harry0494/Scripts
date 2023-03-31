import csv
import json
import re

input_csv_file = 'Onboarded_Workers.csv'
output_csv_file = 'External_Worker_IDs.csv'

pattern = r'\{.*\}'

with open(input_csv_file, 'r') as csv_file, open(output_csv_file, 'w', newline = '') as output_csv:
    csv_reader = csv.reader(csv_file)
    csv_writer = csv.writer(output_csv)

    for row in csv_reader:
        value_set = row[0]

        match = re.search(pattern, value_set)
        onboard_worker_accepted_hash = match.group(0) 

        onboarded_worker_dict = json.loads(onboard_worker_accepted_hash)

        external_worker_ids = onboarded_worker_dict['eventDetail']['workerId']

        csv_writer.writerow(["'" + external_worker_ids + "',"])
