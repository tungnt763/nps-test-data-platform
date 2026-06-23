import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime, timedelta
import random

# Tạo fake transaction data
rows = []
banks = ['VCB', 'BIDV', 'TCB', 'MB', 'VPB']
for i in range(1000):
  rows.append({
    'txn_id': f'TXN{i:08d}',
    'bank_id': random.choice(banks),
    'amount': round(random.uniform(10000, 5000000), 2),
    'merchant_id': f'MERCH{random.randint(1,100):04d}',
    'status_code': '00' if random.random() > 0.05 else '51',
    'created_at': datetime(2026, 6, 19, 0, 0, 0) + timedelta(seconds=random.randint(0, 86400)),
    'card_number': f'****-****-****-{random.randint(1000,9999)}'
  })

df = pd.DataFrame(rows)
table = pa.Table.from_pandas(df)
import os
output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'transactions-2026-06-19.parquet')
pq.write_table(table, output_path)
print(f"File saved to: {output_path}")
print(f"Created {len(rows)} transactions")