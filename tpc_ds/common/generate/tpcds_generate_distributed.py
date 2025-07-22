# -*- coding: utf-8 -*-

import argparse
import subprocess
from pyspark.sql import SparkSession
import os
import tempfile
import logging
import shutil
import uuid


# =============================
# Конфигурация логирования
# =============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("tpcds_generator")

# =============================
# Список всех таблиц TPC-DS
# =============================
TPCDS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site"
]

# Таблицы, которые НЕ нужно параллелить
NON_PARALLEL_TABLES = {
    "call_center", "catalog_page", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "item", "promotions", "reason",
    "ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site"
}

# Связь родитель -> дочерняя таблица
CHILD_TABLES_MAP = {
    "catalog_sales": "catalog_returns",
    "store_sales": "store_returns",
    "web_sales": "web_returns"
}

def get_executor_tmp_dir():
    spark_local_dirs = os.environ.get("SPARK_WORKER_DIR", "/tmp/tpc_cache")
    base_dir = spark_local_dirs.split(",")[0]
    tmp_dir = os.path.join(base_dir, f"tpcds_tmp_{uuid.uuid4().hex}")
    os.makedirs(tmp_dir, exist_ok=True)
    return tmp_dir

def generate_partition(partition_id, table_name, scale, parallel, dsdgen_path, dist_path, delimiter):
    tmpdir = get_executor_tmp_dir()

    try:
        if parallel == 1:
            output_file = os.path.join(tmpdir, f"{table_name}.dat")
            cmd = [
                dsdgen_path,
                "-TABLE", table_name,
                "-SCALE", str(scale),
                "-DISTRIBUTIONS", dist_path,
                "-DELIMITER", f"{delimiter}",
                "-DIR", tmpdir,
                "-FORCE", "Y",
                "-TERMINATE", "N"
            ]
        else:
            output_file = os.path.join(tmpdir, f"{table_name}_{partition_id + 1}_{parallel}.dat")
            cmd = [
                dsdgen_path,
                "-TABLE", table_name,
                "-SCALE", str(scale),
                "-PARALLEL", str(parallel),
                "-CHILD", str(partition_id + 1),
                "-DISTRIBUTIONS", dist_path,
                "-DELIMITER", f"{delimiter}",
                "-DIR", tmpdir,
                "-FORCE", "Y",
                "-TERMINATE", "N"
            ]

        print(f"[INFO] Running dsdgen: {' '.join(cmd)}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode != 0:
            raise RuntimeError(
                f"[ERROR] dsdgen failed for table {table_name}, partition {partition_id}.\n"
                f"Return code: {result.returncode}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

        if os.path.isfile(output_file):
            print(f"[INFO] Successfully generated: {output_file}")
            with open(output_file, "rb") as f:
                for raw_line in f:
                    try:
                        line = raw_line.decode('latin-1').strip()
                        row = line.split(delimiter)
                        yield row
                    except UnicodeDecodeError as e:
                        logger.error(f"Ошибка декодирования строки в {output_file}: {e}")
                        continue
        else:
            print(f"[WARN] File not found: {output_file} (probably empty partition)")

    finally:
        print(f"[INFO] Cleaning up temporary directory: {tmpdir}")
        shutil.rmtree(tmpdir, ignore_errors=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsdgen-path", required=True)
    parser.add_argument("--dist-path", required=True)
    parser.add_argument("--scale", type=int, default=1)
    parser.add_argument("--parallel", type=int, default=4)
    parser.add_argument("--delimiter", default="|")
    parser.add_argument("--output-path", required=True, help="HDFS or S3 path")
    parser.add_argument("--format", choices=["parquet", "csv", "orc", "iceberg"], default="parquet")

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("TPC-DS Distributed Generator") \
        .getOrCreate()

    for table in TPCDS_TABLES:
        if table in CHILD_TABLES_MAP.values():
            continue  # Не генерируем дочерние таблицы отдельно

        table_parallel = 1 if table in NON_PARALLEL_TABLES else args.parallel
        mode = "SEQUENTIAL" if table_parallel == 1 else "PARALLEL"
        print(f"Generating table: {table} ({mode} mode, {table_parallel} partitions)")

        rdd = spark.sparkContext.parallelize(range(table_parallel), table_parallel)

        rows_rdd = rdd.flatMap(lambda idx: generate_partition(
            idx, table, args.scale, table_parallel,
            args.dsdgen_path, args.dist_path, args.delimiter))

        tables_to_process = [table]
        if table in CHILD_TABLES_MAP:
            tables_to_process.append(CHILD_TABLES_MAP[table])

        for t in tables_to_process:
            if rows_rdd.isEmpty():
                print(f"Skipping table {t} (empty RDD)")
                continue

            df = spark.createDataFrame(rows_rdd)
            output_table_path = os.path.join(args.output_path, t)
            print(f"Saving table {t} to {output_table_path}")

            if args.format == "parquet":
                df.write.mode("overwrite").parquet(output_table_path)
            
            elif args.format == "orc":
                df.write.mode("overwrite").orc(output_table_path)
            
            elif args.format == "csv":
                df.write.mode("overwrite").option("delimiter", args.delimiter).csv(output_table_path)
            
            elif args.format == "iceberg":
                # Требует настроенный Spark с поддержкой Iceberg (через catalog)
                table_name = f"{t}"
                print(f"[INFO] Writing Iceberg table {table_name}")
                df.writeTo(table_name).using("iceberg").tableProperty("format-version", "2").createOrReplace()
            
            else:
                raise ValueError(f"Unsupported format: {args.format}")


    spark.stop()

if __name__ == "__main__":
    main()
