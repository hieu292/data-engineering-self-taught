/*
  Không chuyến taxi nào chạy quá 12 tiếng. Nghe hiển nhiên tới mức thừa —
  cho tới lúc nó đỏ, và bạn phát hiện đồng hồ tính tiền quên tắt qua đêm.

  Bài test này gác một giả định mà bộ lọc ở silver KHÔNG bắt: ở đó chỉ kiểm
  `dropoff > pickup`, nghĩa là một chuyến 30 tiếng vẫn lọt.
*/

select
    trip_id,
    pickup_at,
    dropoff_at,
    (unix_timestamp(dropoff_at) - unix_timestamp(pickup_at)) / 3600 as hours
from {{ ref('silver_trips') }}
where unix_timestamp(dropoff_at) - unix_timestamp(pickup_at) > 12 * 3600
