{#
  Mặc định dbt ghép tên schema thành `<schema mặc định>_<schema khai trong model>`
  → ra `silver_gold`, `silver_sandbox`. Quy ước đó sinh ra để nhiều người cùng
  chạy dbt trên MỘT kho dữ liệu mà không giẫm lên nhau (mỗi người một prefix).

  Ở đây ta muốn đúng ba schema `bronze` / `silver` / `gold` — tên phải khớp với
  kiến trúc medallion thì sơ đồ mới đọc được. Ghi đè macro chuẩn của dbt là
  cách chính tắc để làm việc đó, không phải mẹo vặt.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
