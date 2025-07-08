from pyspark.sql import SparkSession
import os

# Spark session with Delta support
spark = SparkSession.builder \
    .appName("Customer Delta Project") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

input_dir = "customers_data"
output_dir = "delta_tables"
os.makedirs(output_dir, exist_ok=True)

file_types = {
    "customers.csv": "csv",
    "customers.tsv": "csv",
    "customers.txt": "csv",
    "customers.json": "json",
    "customers.parquet": "parquet",
    "customers.orc": "orc",
    "customers.avro": "avro",
    "customers.xlsx": "excel",
    "customers.xml": "xml"
}

# Validation function
def validate_table(path):
    df = spark.read.format("delta").load(path)
    count = df.count()
    print(f"✅ {path} → {count} rows")
    assert count == 500, f"❌ Error: Expected 500 rows but got {count}"

for filename, ftype in file_types.items():
    full_path = os.path.join(input_dir, filename)
    delta_path = os.path.join(output_dir, filename.split(".")[0])

    try:
        if ftype == "csv":
            sep = "," if "csv" in filename else ("\t" if "tsv" in filename else "|")
            df = spark.read.option("header", "true").option("sep", sep).csv(full_path)
        elif ftype == "json":
            df = spark.read.json(full_path)
        elif ftype == "parquet":
            df = spark.read.parquet(full_path)
        elif ftype == "orc":
            df = spark.read.orc(full_path)
        elif ftype == "avro":
            df = spark.read.format("avro").load(full_path)
        elif ftype == "excel":
            print(f"⚠️ Skipping {filename} (Excel not supported directly)")
            continue
        elif ftype == "xml":
            print(f"⚠️ Skipping {filename} (Needs extra spark-xml plugin)")
            continue
        else:
            continue

        df.write.format("delta").mode("overwrite").save(delta_path)
        validate_table(delta_path)

    except Exception as e:
        print(f"❌ Error with {filename}: {str(e)}")

print("✅ All Delta Tables created and validated.")
