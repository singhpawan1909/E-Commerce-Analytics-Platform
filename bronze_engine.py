"""
Bronze Engine — Generic CSV-to-Delta ingestion.
All Delta files write to S3 (not Databricks managed storage).
"""
from pyspark.sql.functions import current_timestamp, lit
from table_config import DIMENSION_TABLES, FACT_TABLES, S3_RAW, S3_LIVE, S3_DELTA_BRONZE


class BronzeEngine:
    

    def __init__(self, spark):
        self.spark = spark
        self.results = {}

    def _read_csv(self, s3_path):
        return self.spark.read.option("header", True).option("inferSchema", True).csv(s3_path)

    def _add_audit(self, df, source_file, batch_id):
        return df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(source_file)) \
            .withColumn("batch_id", lit(batch_id))

    def _write_delta(self, df, table_name, mode):
        """Write Delta to S3 and register as Spark table."""
        s3_path = f"{S3_DELTA_BRONZE}/{table_name}"
        df.write.format("delta").mode(mode).save(s3_path)

        if mode == "overwrite":
            self.spark.sql(f"CREATE OR REPLACE TABLE bronze.{table_name} USING DELTA LOCATION '{s3_path}'")
        else:
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.{table_name} USING DELTA LOCATION '{s3_path}'")

        count = df.count()
        self.results[table_name] = self.results.get(table_name, 0) + count
        print(f"    bronze.{table_name} — {count:,} rows ({mode}) → {s3_path}")

    def ingest_table(self, table_name, config, s3_folder, batch_id, mode):
        source = config["source_file"]
        try:
            df = self._read_csv(f"{s3_folder}/{source}")
            df = self._add_audit(df, source, batch_id)
            self._write_delta(df, table_name, mode)
        except Exception as e:
            print(f"    ERROR: {table_name} — {e}")

    def ingest_dimensions(self):
        print("\n  DIMENSIONS (full load — overwrite)")
        for name, cfg in DIMENSION_TABLES.items():
            self.ingest_table(name, cfg, f"{S3_RAW}/batch_1", "batch_1", "overwrite")

    def ingest_facts(self, batch_number):
        mode = "overwrite" if batch_number == "1" else "append"
        print(f"\n  FACTS — batch {batch_number} ({mode})")
        for name, cfg in FACT_TABLES.items():
            self.ingest_table(name, cfg, f"{S3_RAW}/batch_{batch_number}", f"batch_{batch_number}", mode)

    def ingest_live(self):
        print("\n  LIVE STREAM (append to Bronze)")
        for name, cfg in FACT_TABLES.items():
            self.ingest_table(name, cfg, S3_LIVE, "live_stream", "append")

    def run(self, batch_number):
        print("=" * 60)
        print(f"BRONZE ENGINE — batch: {batch_number}")
        print("=" * 60)
        self.results = {}

        if batch_number == "1":
            self.ingest_dimensions()
            self.ingest_facts("1")
        elif batch_number in ["2", "3", "4"]:
            self.ingest_facts(batch_number)
        elif batch_number == "live":
            self.ingest_live()
        else:
            raise ValueError(f"Invalid batch: {batch_number}")

        total = sum(self.results.values())
        print(f"\nBRONZE COMPLETE — {total:,} rows")
