import pandas as pd
from dicttoxml import dicttoxml
from fastavro import writer, parse_schema
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json

# Sample data
data = {
    "CustomerID": range(1, 501),
    "FirstName": [f"First{i}" for i in range(1, 501)],
    "LastName": [f"Last{i}" for i in range(1, 501)],
    "Email": [f"user{i}@email.com" for i in range(1, 501)],
    "Country": ["India"] * 500
}
df = pd.DataFrame(data)
os.makedirs("customers_data", exist_ok=True)

# 1. CSV
df.to_csv("customers_data/customers.csv", index=False)

# 2. TSV
df.to_csv("customers_data/customers.tsv", sep='\t', index=False)

# 3. TXT
df.to_csv("customers_data/customers.txt", sep='|', index=False)

# 4. JSON
df.to_json("customers_data/customers.json", orient='records', lines=True)

# 5. XML
xml_data = dicttoxml(df.to_dict(orient="records"), custom_root='Customers', attr_type=False)
with open("customers_data/customers.xml", "wb") as f:
    f.write(xml_data)

# 6. XLSX
df.to_excel("customers_data/customers.xlsx", index=False)

# 7. Parquet
table = pa.Table.from_pandas(df)
pq.write_table(table, "customers_data/customers.parquet")

# 8. ORC
import pyarrow.orc as orc
with orc.ORCWriter("customers_data/customers.orc") as writer_orc:
    writer_orc.write(table)

# 9. Avro
schema = {
    "doc": "Customer record",
    "name": "Customer",
    "namespace": "example.avro",
    "type": "record",
    "fields": [
        {"name": "CustomerID", "type": "int"},
        {"name": "FirstName", "type": "string"},
        {"name": "LastName", "type": "string"},
        {"name": "Email", "type": "string"},
        {"name": "Country", "type": "string"}
    ]
}
parsed_schema = parse_schema(schema)
records = df.to_dict(orient="records")
with open("customers_data/customers.avro", "wb") as out:
    writer(out, parsed_schema, records)

print("✅ All customer files generated in 'customers_data/' folder.")
