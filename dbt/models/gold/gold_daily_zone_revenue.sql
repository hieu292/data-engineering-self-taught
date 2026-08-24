/*
  GOLD — tầng trả lời câu hỏi nghiệp vụ, không phải tầng lưu dữ liệu.

  Bảng này có sẵn một hình dạng: đủ nhỏ để Superset (Phase 5) vẽ tức thì,
  đủ chi tiết để analyst tự cắt theo ngày / vùng / quận mà không phải hỏi ai.
  36 triệu dòng silver co lại còn khoảng vài chục nghìn dòng.

  Chú ý join: `silver_zones` chỉ 265 dòng nên Spark tự broadcast — không sinh
  shuffle nào. Đúng thứ đã tự tay đo ở bước 6 Phase 3.
*/

with trips as (
    select * from {{ ref('silver_trips') }}
),

zones as (
    select * from {{ ref('silver_zones') }}
)

select
    trips.pickup_date,
    zones.borough,
    zones.zone_name,

    count(*)                                     as trip_count,
    round(sum(trips.total_amount), 2)            as revenue,
    round(avg(trips.total_amount), 2)            as avg_ticket,
    round(avg(trips.trip_miles), 2)              as avg_miles,
    round(avg(trips.trip_minutes), 1)            as avg_minutes,

    -- Tỷ lệ tiền boa CHỈ tính trên chuyến trả bằng thẻ. Chuyến tiền mặt luôn
    -- ghi tip = 0 vì tài xế bỏ túi, hệ thống không thấy. Gộp chung là kéo tụt
    -- con số xuống rồi kết luận "khách New York keo kiệt" — sai hoàn toàn.
    -- Đây là kiểu lỗi mà chỉ người HIỂU dữ liệu mới tránh được, không phải
    -- kiểu lỗi công cụ nào bắt hộ.
    round(
        sum(case when trips.payment_type_id = 1 then trips.tip_amount else 0 end)
        / nullif(sum(case when trips.payment_type_id = 1 then trips.fare_amount else 0 end), 0)
    * 100, 1)                                    as card_tip_pct

from trips
left join zones on trips.pickup_zone_id = zones.zone_id
group by 1, 2, 3
