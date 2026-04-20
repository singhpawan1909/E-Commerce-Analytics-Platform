"""
Silver Engine — Generic cleaning. Reads config, applies rules, writes to S3.
"""
from pyspark.sql.functions import col, trim, initcap, upper, when, to_timestamp, coalesce, lit, avg, first, expr
from table_config import ALL_TABLES, DIMENSION_TABLES, FACT_TABLES, S3_DELTA_BRONZE, S3_DELTA_SILVER


class SilverEngine:
    
    def __init__(self, spark):
        self.spark = spark
        self.results = {}

    def _apply_rule(self, df, rule):
        c = rule["column"]
        a = rule["action"]
        if a == "initcap_trim":    return df.withColumn(c, initcap(trim(col(c))))
        elif a == "upper_trim":    return df.withColumn(c, upper(trim(col(c))))
        elif a == "cast_string":   return df.withColumn(c, expr(f"try_cast(`{c}` as string)"))
        elif a == "cast_double":   return df.withColumn(c, expr(f"try_cast(`{c}` as double)"))
        elif a == "cast_int":      return df.withColumn(c, expr(f"try_cast(`{c}` as int)"))
        elif a == "to_timestamp":  return df.withColumn(c, expr(f"try_cast(`{c}` as timestamp)"))
        elif a == "fill_null":     return df.withColumn(c, coalesce(col(c), lit(rule.get("default", ""))))
        elif a == "replace_value": return df.withColumn(c, when(col(c) == rule["old"], rule["new"]).otherwise(col(c)))
        elif a == "rename":        return df.withColumnRenamed(c, rule["new_name"])
        else:
            print(f"    WARNING: Unknown action '{a}'")
            return df

    def _apply_join(self, df, join_cfg):
        try:
            join_df = self.spark.read.format("delta").load(f"{S3_DELTA_SILVER}/{join_cfg['source_table']}")
        except:
            join_df = self.spark.read.format("delta").load(f"{S3_DELTA_BRONZE}/{join_cfg['source_table']}")
        join_key = join_cfg["on"] if isinstance(join_cfg["on"], list) else [join_cfg["on"]]
        overlap = [c for c in join_df.columns if c in df.columns and c not in join_key]
        if overlap:
            join_df = join_df.drop(*overlap)
        df = df.join(join_df, join_cfg["on"], join_cfg.get("how", "left"))
        for c, d in join_cfg.get("fill_after", {}).items():
            df = df.withColumn(c, coalesce(col(c), lit(d)))
        return df

    def _apply_aggregate(self, df, agg_cfg):
        agg_exprs = []
        for c, func in agg_cfg["aggs"].items():
            if func == "avg":   agg_exprs.append(avg(c).alias(c))
            elif func == "first": agg_exprs.append(first(c).alias(c))
        return df.groupBy(*agg_cfg["group_by"]).agg(*agg_exprs)

    def _write_delta(self, df, table_name):
        s3_path = f"{S3_DELTA_SILVER}/{table_name}"
        df.write.format("delta").mode("overwrite").save(s3_path)
        self.spark.sql(f"DROP TABLE IF EXISTS silver.{table_name}")
        self.spark.sql(f"CREATE TABLE silver.{table_name} USING DELTA LOCATION '{s3_path}'")
        count = df.count()
        self.results[table_name] = count
        print(f"    silver.{table_name} — {count:,} rows → {s3_path}")

    def transform_table(self, table_name, config):
        print(f"\n  Transforming: {table_name}")
        try:
            df = self.spark.read.format("delta").load(f"{S3_DELTA_BRONZE}/{table_name}")
        except Exception as e:
            print(f"    ERROR: {e}")
            return

        for rule in config.get("cleaning_rules", []):
            df = self._apply_rule(df, rule)

        if "join" in config:
            df = self._apply_join(df, config["join"])
        if "aggregate" in config:
            df = self._apply_aggregate(df, config["aggregate"])

        dedup = config.get("dedup_keys", [])
        if dedup:
            df = df.dropDuplicates(dedup)

        self._write_delta(df, table_name)

    def run(self):
        print("=" * 60)
        print("SILVER ENGINE — Transforming all tables")
        print("=" * 60)
        self.results = {}

        for name, cfg in DIMENSION_TABLES.items():
            self.transform_table(name, cfg)
        for name, cfg in FACT_TABLES.items():
            self.transform_table(name, cfg)

        print(f"\nSILVER COMPLETE")
        for t, c in self.results.items():
            print(f"    {t}: {c:,}")
