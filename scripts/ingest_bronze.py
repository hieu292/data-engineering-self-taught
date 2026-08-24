#!/usr/bin/env python3
"""
Phase 4 · Bước ingest: raw (Parquet trần trên MinIO) → bronze (bảng Delta có tên).

VÌ SAO KHÔNG PHẢI MỘT MODEL dbt?
dbt là chữ **T** trong ELT — nó biến đổi dữ liệu ĐÃ nằm trong kho. Việc kéo file
từ ngoài vào là chữ **E/L**, thuộc về công cụ khác (ở đời thật: Fivetran, Airbyte,
Spark job, hoặc Auto Loader của Databricks). Trộn hai việc vào dbt là lỗi kiến
trúc thường gặp, và nó phá luôn khả năng test: dbt không test nổi thứ nó tự nạp.

QUY TẮC CỦA TẦNG BRONZE — chép nguyên trạng, không sửa gì:
  • không đổi tên cột (VendorID vẫn hoa lẫn thường — xấu nhưng đúng nguồn)
  • không ép kiểu, không lọc dòng rác
  • CHỈ thêm cột metadata bắt đầu bằng `_`
Lý do: bronze là bản sao có thể đối chiếu với nguồn. Ngày nào silver ra số lạ,
bạn cần một chỗ chắc chắn chưa ai đụng vào để lần ngược.

Chạy:  make ingest              (12 tháng)
       make ingest MONTH=2024-03   (một tháng)
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
RAW = f"s3a://{BUCKET}/raw/yellow_taxi"
BRONZE_LOCATION = f"s3a://{BUCKET}/phase4/bronze/yellow_trips"

months = sys.argv[1:] or [f"2024-{m:02d}" for m in range(1, 13)]

spark = SparkSession.builder.appName("ingest_bronze").getOrCreate()
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

for month in months:
    path = f"{RAW}/yellow_tripdata_{month}.parquet"
    df = (
        spark.read.parquet(path)
        # Ba cột metadata này là thứ DUY NHẤT bronze được phép thêm.
        # `_source_file` cho phép truy ngược một dòng bẩn về đúng file gốc.
        # `_ingested_at` phân biệt "dữ liệu xảy ra lúc nào" với "ta biết nó lúc nào".
        .withColumn("_source_file", F.lit(f"yellow_tripdata_{month}.parquet"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_pickup_month", F.lit(month))
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        # replaceWhere: ghi đè ĐÚNG một partition, giữ nguyên 11 tháng kia.
        # Đây là chìa khoá của tính idempotent — chạy lại tháng 3 mười lần
        # vẫn ra đúng một bản tháng 3, không nhân đôi. Phase 7 sẽ dựa vào
        # đúng tính chất này để backfill.
        .option("replaceWhere", f"_pickup_month = '{month}'")
        .option("mergeSchema", "true")
        .partitionBy("_pickup_month")
        .option("path", BRONZE_LOCATION)
        .saveAsTable("bronze.yellow_trips")
    )
    print(f"  ✓ {month}")

total = spark.table("bronze.yellow_trips").count()
print(f"bronze.yellow_trips: {total:,} dòng, {len(months)} tháng vừa nạp")
spark.stop()
