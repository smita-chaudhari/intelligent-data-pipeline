import csv
import random
from datetime import datetime, timedelta

def generate_transaction_logs(file_name, num_rows):
    regions = ['US', 'EU', 'APAC']
    securities = ['Bond', 'Equity', 'ETF', 'Derivative']
    start_date = datetime(2023, 1, 1)

    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['transaction_id', 'timestamp', 'region', 'security_type', 'amount'])

        for i in range(num_rows):
            txn_id = f"T{i:010d}"
            date = start_date + timedelta(seconds=random.randint(0, 31536000))
            region = random.choice(regions)
            sec_type = random.choice(securities)
            amount = round(random.uniform(100, 100000), 2)

            writer.writerow([txn_id, date.isoformat(), region, sec_type, amount])

generate_transaction_logs('sample_transactions.csv', 500)

