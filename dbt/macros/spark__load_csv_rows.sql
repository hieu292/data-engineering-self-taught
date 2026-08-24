{#
  ═══ VÁ MỘT LỖI CỦA ADAPTER, NGAY TỪ DỰ ÁN CỦA MÌNH ═══

  Macro gốc `spark__load_csv_rows` của dbt-spark sinh câu INSERT dùng tham số
  `%s` rồi để lớp bên dưới thay giá trị vào. Với kết nối `method: session`, lớp
  "bên dưới" đó chỉ làm đúng một việc: `sql % parameters` — nội suy chuỗi trần,
  không đặt dấu nháy, không thoát ký tự.

  Hậu quả: chỉ cần một tên vùng có dấu nháy đơn — "Governor's Island" —
  là cả câu lệnh vỡ:
      [PARSE_SYNTAX_ERROR] Syntax error at or near 's'

  Bản vá này viết thẳng giá trị vào câu SQL và tự thoát ký tự. dbt cho phép ghi
  đè BẤT KỲ macro nào của adapter chỉ bằng cách khai lại đúng tên trong thư mục
  macros/ của dự án — không cần fork, không cần chờ bản vá thượng nguồn. Đây là
  một trong những khả năng thực dụng nhất của dbt, và là lý do người ta chịu
  được chuyện adapter có bug.

  (Sự cố này chỉ xảy ra với `method: session`. Đường thrift/odbc có lớp driver
  lo việc đặt tham số tử tế. Đánh đổi của việc bỏ Thrift Server đi.)
#}

{% macro spark__load_csv_rows(model, agate_table) %}

  {% set batch_size = get_batch_size() %}
  {% set column_override = model['config'].get('column_types', {}) %}
  {% set statements = [] %}

  {% for chunk in agate_table.rows | batch(batch_size) %}

      {% set sql %}
          insert into {{ this.render() }} values
          {% for row in chunk -%}
              ({%- for col_name in agate_table.column_names -%}
                  {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
                  {%- set type = column_override.get(col_name, inferred_type) -%}
                  {%- set value = row[loop.index0] -%}
                  {%- if value is none -%}
                      cast(null as {{ type }})
                  {%- else -%}
                      {#- Thoát dấu chéo ngược TRƯỚC rồi mới tới dấu nháy — đảo
                          thứ tự là tự thoát lại phần mình vừa thêm vào. -#}
                      {%- set escaped = value | string | replace('\\', '\\\\') | replace("'", "\\'") -%}
                      cast('{{ escaped }}' as {{ type }})
                  {%- endif -%}
                  {%- if not loop.last %},{% endif %}
              {%- endfor -%})
              {%- if not loop.last %},{% endif %}
          {%- endfor %}
      {% endset %}

      {% do adapter.add_query(sql, abridge_sql_log=True) %}

      {% if loop.index0 == 0 %}
          {% do statements.append(sql) %}
      {% endif %}
  {% endfor %}

  {{ return(statements[0]) }}
{% endmacro %}
