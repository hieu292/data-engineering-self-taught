#!/usr/bin/env python3
"""Phase 6 · Đăng ký metadata các bảng thật vào Unity Catalog.

VÌ SAO SCRIPT NÀY TỒN TẠI: `unitycatalog-spark` (plugin catalog Spark cắm vào
Unity Catalog) không đọc/ghi được dữ liệu qua MinIO — nó luôn tự vend
"temporary credentials" qua REST, và bản UC OSS phát hành chính thức chưa hỗ
trợ custom S3 endpoint trong luồng đó (xem docker-compose.yml, service
unity-catalog, và docs/roadmap.md Phần 9). Script này ĐI VÒNG đúng cái vòng
đó: gọi thẳng REST API của Unity Catalog để đăng ký METADATA (tên bảng, cột,
kiểu dữ liệu, đường dẫn) — không đụng tới dữ liệu, nên không cần credential
vending. Dữ liệu thật vẫn do Spark/dbt ghi qua Hive Metastore như mọi phase
trước; Unity Catalog chỉ biết "có bảng này, cột này, ở đây" — đúng vai trò
GOVERNANCE, tách khỏi vai trò THỰC THI.

Chạy:  make uc-register   (sau khi `make dbt` đã dựng xong bảng thật)
"""
import json
import os
import urllib.error
import urllib.request

from pyspark.sql import SparkSession

UC_URL = os.environ["UNITY_CATALOG_URL"]
UC_TOKEN = os.environ["UC_ADMIN_TOKEN"]
CATALOG = "unity"

# schema → (table, có phải seed/managed không — seed không khai location_root)
TABLES = [
    ("bronze", "yellow_trips"),
    ("silver", "silver_trips"),
    ("silver", "silver_zones"),
    ("silver", "taxi_zones"),
    ("gold", "gold_daily_zone_revenue"),
    ("gold", "gold_hourly_demand"),
]

# Spark simpleString() → (UC type_name enum, UC type_text)
_SIMPLE_TYPES = {
    "int": ("INT", "int"),
    "bigint": ("LONG", "bigint"),
    "smallint": ("SHORT", "smallint"),
    "tinyint": ("BYTE", "tinyint"),
    "double": ("DOUBLE", "double"),
    "float": ("FLOAT", "float"),
    "string": ("STRING", "string"),
    "boolean": ("BOOLEAN", "boolean"),
    "date": ("DATE", "date"),
    "timestamp": ("TIMESTAMP", "timestamp"),
    "timestamp_ntz": ("TIMESTAMP_NTZ", "timestamp_ntz"),
    "binary": ("BINARY", "binary"),
}


def map_type(spark_type):
    simple = spark_type.simpleString()
    if simple in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[simple]
    if simple.startswith("decimal"):
        return "DECIMAL", simple
    raise ValueError(f"Chưa map kiểu Spark '{simple}' sang kiểu Unity Catalog")


def _parse_json(raw):
    # DELETE thành công trả về "null" trần (không phải object) — không lỗi,
    # chỉ không phải thứ json.loads(...) mong đợi khi ta luôn coi payload là dict.
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {"_raw": raw.decode(errors="replace")}
    return parsed if isinstance(parsed, dict) else {"_raw": parsed}


def uc_request(method, path, body=None, ok_codes=(200, 201)):
    req = urllib.request.Request(
        f"{UC_URL}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={
            "Authorization": f"Bearer {UC_TOKEN}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, _parse_json(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, _parse_json(e.read())


def ensure_catalog(name):
    status, body = uc_request("POST", "/api/2.1/unity-catalog/catalogs", {"name": name})
    if status not in (200, 201) and "ALREADY_EXISTS" not in body.get("error_code", ""):
        raise RuntimeError(f"Tạo catalog {name} lỗi: {body}")
    print(f"✓ catalog {name}")


def ensure_schema(catalog, schema):
    status, body = uc_request(
        "POST",
        "/api/2.1/unity-catalog/schemas",
        {"name": schema, "catalog_name": catalog},
    )
    if status not in (200, 201) and "ALREADY_EXISTS" not in body.get("error_code", ""):
        raise RuntimeError(f"Tạo schema {catalog}.{schema} lỗi: {body}")
    print(f"✓ schema {catalog}.{schema}")


def register_table(spark, catalog, schema, table):
    full = f"{schema}.{table}"
    df = spark.table(full)
    detail = spark.sql(f"DESCRIBE DETAIL {full}").collect()[0]
    # Bảng thật nằm ở s3a:// (Hive Metastore/Hadoop) — Unity Catalog chỉ nhận
    # scheme s3:// (nghĩ theo kiểu AWS thật, xem docs/roadmap.md Phần 9). Đây
    # CHỈ đổi chuỗi hiển thị trong metadata đăng ký, không đụng file thật —
    # script này không đọc/ghi dữ liệu, chỉ mô tả nó.
    location = detail["location"].replace("s3a://", "s3://", 1)

    columns = []
    for i, field in enumerate(df.schema.fields):
        type_name, type_text = map_type(field.dataType)
        columns.append(
            {
                "name": field.name,
                "type_text": type_text,
                "type_name": type_name,
                "type_json": json.dumps(field.jsonValue()),
                "position": i,
                "nullable": field.nullable,
            }
        )

    # Xoá đăng ký cũ nếu có — script này chạy lại được sau mỗi `make dbt`,
    # không tích luỹ bản ghi lệch schema.
    uc_request("DELETE", f"/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}")

    status, body = uc_request(
        "POST",
        "/api/2.1/unity-catalog/tables",
        {
            "name": table,
            "catalog_name": catalog,
            "schema_name": schema,
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": location,
            "columns": columns,
        },
    )
    if status not in (200, 201):
        raise RuntimeError(f"Đăng ký bảng {full} lỗi: {body}")
    print(f"✓ table  {catalog}.{full}  ({len(columns)} cột, {location})")


def main():
    spark = SparkSession.builder.getOrCreate()

    ensure_catalog(CATALOG)
    for schema in {"bronze", "silver", "gold"}:
        ensure_schema(CATALOG, schema)

    for schema, table in TABLES:
        register_table(spark, CATALOG, schema, table)

    print(f"\nXong. Xem qua: docker compose exec trino ... hoặc "
          f"bin/uc table list --catalog {CATALOG} --schema gold "
          f"--server {UC_URL} --auth_token <token>")


if __name__ == "__main__":
    main()
