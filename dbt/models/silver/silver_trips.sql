{{
  config(
    materialized   = 'incremental',
    incremental_strategy = 'merge',
    unique_key     = 'trip_id',
    partition_by   = 'pickup_month',
    on_schema_change = 'append_new_columns'
  )
}}

/*
  SILVER — nơi dữ liệu trở nên DÙNG ĐƯỢC.
  Ba việc, không hơn: đặt tên tử tế, ép đúng kiểu, vứt dòng vô lý.
  Tuyệt đối không tổng hợp (aggregate) ở đây — đó là việc của gold. Silver phải
  giữ nguyên độ mịn "một dòng = một chuyến", vì mọi câu hỏi chưa nghĩ ra hôm nay
  đều sẽ phải quay về tầng này.

  `materialized = incremental`: lần đầu tạo bảng từ đầu; những lần sau chỉ xử lý
  phần mới. Nhờ vậy chạy lại hàng ngày mất vài giây thay vì vài phút.
  `strategy = merge` + `unique_key`: dbt sinh ra câu MERGE của Delta — dòng đã có
  thì cập nhật, chưa có thì chèn. Đó chính là thứ khiến pipeline IDEMPOTENT:
  chạy mười lần ra đúng một kết quả. Bước 6 sẽ cho thấy bỏ nó đi thì sao.
*/

with source as (

    select * from {{ source('bronze', 'yellow_trips') }}

    {% if is_incremental() %}
    -- Khối này CHỈ xuất hiện khi bảng đã tồn tại. Lần chạy đầu tiên dbt bỏ qua nó
    -- và quét toàn bộ bronze. `{{ this }}` là chính bảng đang được xây.
    where tpep_pickup_datetime >= (select coalesce(max(pickup_at), '1900-01-01') from {{ this }})
    {% endif %}

),

renamed as (

    select
        /*
          KHÓA THAY THẾ (surrogate key).

          Dữ liệu TLC không có mã chuyến — không có gì để MERGE lên. Ta tự chế
          bằng cách băm những cột mà hai chuyến khác nhau gần như không thể trùng
          hết. Đây là kỹ thuật chuẩn; trong đời thật người ta gọi macro
          `dbt_utils.generate_surrogate_key`, ở đây viết tay để nhìn thấy ruột nó.

          Hai chi tiết dễ sai:
          • concat_ws chứ không phải concat: `concat` gặp NULL trả về NULL, mất
            trắng cả khoá.
          • Phải có dấu ngăn giữa các trường, nếu không '1'+'23' và '12'+'3'
            băm ra cùng một khoá.
        */
        md5(concat_ws('|',
            cast(VendorID as string),
            cast(tpep_pickup_datetime as string),
            cast(tpep_dropoff_datetime as string),
            cast(PULocationID as string),
            cast(DOLocationID as string),
            cast(total_amount as string)
        )) as trip_id,

        cast(VendorID as int)                as vendor_id,
        cast(tpep_pickup_datetime as timestamp)  as pickup_at,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_at,
        cast(PULocationID as int)            as pickup_zone_id,
        cast(DOLocationID as int)            as dropoff_zone_id,
        cast(passenger_count as int)         as passenger_count,
        cast(trip_distance as double)        as trip_miles,
        cast(payment_type as int)            as payment_type_id,
        cast(fare_amount as double)          as fare_amount,
        cast(tip_amount as double)           as tip_amount,
        cast(tolls_amount as double)         as tolls_amount,
        cast(total_amount as double)         as total_amount,

        /*
          Cột dẫn xuất: tính MỘT lần ở silver để hàng chục truy vấn phía sau khỏi
          mỗi nơi tính một kiểu rồi ra số khác nhau.

          BẪY ĐÃ GỠ: viết `dropoff - pickup` cho gọn thì Spark trả về kiểu
          INTERVAL, và Delta ném DELTA_UNSUPPORTED_DATA_TYPES — định dạng Delta
          không có chỗ cho kiểu đó. Phải quy về số. Đây là một trong vài chỗ
          hiếm hoi mà "bảng Delta" hẹp hơn "DataFrame Spark": không phải kiểu nào
          Spark tính được cũng lưu xuống bảng được.
        */
        round((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0, 2) as trip_minutes,
        date(tpep_pickup_datetime)           as pickup_date,
        date_format(tpep_pickup_datetime, 'yyyy-MM') as pickup_month,
        hour(tpep_pickup_datetime)           as pickup_hour,

        _source_file,
        _ingested_at

    from source

),

cleaned as (

    select *
    from renamed
    where
        /*
          BỘ LỌC RÁC. Mỗi dòng dưới đây là một thứ có THẬT trong dữ liệu TLC,
          không phải phòng xa. Chạy `make dq` (bước 3 của notebook) để tự đếm.

          Nguyên tắc: silver LỌC, không SỬA. Đoán xem một chuyến giá âm "đáng lẽ"
          là bao nhiêu chính là bịa dữ liệu. Dòng bị loại vẫn nằm nguyên ở
          bronze — cần thì mở ra xem, không mất đi đâu.
        */
        pickup_at >= '{{ var("start_date") }}'          -- có chuyến ghi năm 2002 và 2090
        and pickup_at <  '{{ var("end_date") }}'
        and dropoff_at > pickup_at                       -- có chuyến trả khách TRƯỚC khi đón
        and trip_miles > 0                               -- ~1.5% chuyến ghi 0 dặm
        and trip_miles < 500                             -- có chuyến ghi 300.000 dặm
        and fare_amount >= 0                             -- giá âm là giao dịch huỷ/hoàn tiền
        and total_amount >= 0
        and passenger_count > 0                          -- 0 khách thì chở cái gì
        and pickup_zone_id between 1 and 265
        and dropoff_zone_id between 1 and 265

        -- Trần thời lượng chuyến. Bộ lọc `dropoff > pickup` ở trên KHÔNG bắt
        -- được chuyến 30 tiếng — đồng hồ quên tắt qua đêm vẫn thoả điều kiện đó.
        -- Ngưỡng để trong biến để bước 5 của notebook tháo ra được và nhìn thấy
        -- bài test `assert_trip_duration_sane` đỏ lên:
        --     dbt build --vars '{max_trip_hours: 999}'
        and unix_timestamp(dropoff_at) - unix_timestamp(pickup_at)
            <= {{ var('max_trip_hours') }} * 3600

),

deduplicated as (

    /*
      KHỬ TRÙNG — bước này KHÔNG có trong bản đầu tiên, và bài test `unique`
      trên `trip_id` đã bắt được: **4 khoá trùng** trong 36 triệu dòng.

      Không phải lỗi của hàm băm. Đó là những dòng trùng nhau HOÀN TOÀN trong
      dữ liệu gốc của TLC — cùng xe, cùng giây đón, cùng giây trả, cùng số tiền.
      Gần như chắc chắn là một sự cố nạp dữ liệu ở đầu bên kia. 4 trên 36 triệu
      là tỷ lệ chẳng ảnh hưởng tới doanh thu, nhưng nó phá đúng thứ quan trọng:

        MERGE ĐÒI KHOÁ PHẢI DUY NHẤT. Một khoá khớp hai dòng nguồn thì Delta
        không có cách nào biết phải giữ dòng nào — nó ném lỗi và cả pipeline
        dừng. Nghĩa là 4 dòng thừa này đủ sức làm hỏng job hàng đêm.

      `row_number` là cách khử trùng chuẩn: xếp hạng trong từng nhóm khoá rồi
      giữ hạng nhất. Nó tốn một shuffle (window function nào cũng vậy) — cái
      giá phải trả để MERGE hoạt động được.
    */

    select * from (
        select
            *,
            row_number() over (partition by trip_id order by _ingested_at) as _rn
        from cleaned
    )
    where _rn = 1

)

select * except (_rn) from deduplicated
