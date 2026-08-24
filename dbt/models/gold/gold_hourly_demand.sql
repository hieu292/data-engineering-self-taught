/*
  "Giờ nào đông khách nhất, ở quận nào?" — câu hỏi điều phối xe kinh điển.
  Bảng cực nhỏ (24 giờ × 7 thứ × 6 quận ≈ 1000 dòng) nhưng đủ vẽ heatmap.
*/

with trips as (
    select * from {{ ref('silver_trips') }}
),

zones as (
    select * from {{ ref('silver_zones') }}
)

select
    zones.borough,
    date_format(trips.pickup_at, 'EEEE')  as day_of_week,
    trips.pickup_hour,

    count(*)                              as trip_count,
    round(avg(trips.trip_miles), 2)       as avg_miles,
    round(sum(trips.total_amount), 2)     as revenue

from trips
left join zones on trips.pickup_zone_id = zones.zone_id
group by 1, 2, 3
