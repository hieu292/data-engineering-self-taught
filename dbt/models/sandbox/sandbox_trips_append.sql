{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append'
  )
}}

/*
  ═══ SAI CÓ CHỦ ĐÍCH — model này CỐ TÌNH hỏng. Đừng sửa nó. ═══

  Khác `silver_trips` đúng hai chỗ:
    • strategy = append  (thay vì merge)
    • không có unique_key

  Trông vô hại. `append` còn NHANH HƠN merge vì khỏi phải dò xem dòng đã tồn
  tại chưa. Rất nhiều pipeline ngoài đời được viết đúng như thế này.

  Vấn đề chỉ lộ ra khi có ai đó chạy lại pipeline — mà chạy lại là chuyện xảy
  ra liên tục: job lỗi giữa chừng, cần backfill, Airflow tự retry, hoặc đơn
  giản là bạn bấm nhầm hai lần. Mỗi lần chạy lại, phần dữ liệu chồng lấn được
  CHÈN THÊM một lần nữa. Không có lỗi nào báo. Không có test mặc định nào đỏ.
  Chỉ có doanh thu tự nhiên tăng gấp đôi, và ba tuần sau kế toán gọi điện.

  Đây chính là khái niệm **idempotency** — chạy N lần cho kết quả y hệt chạy 1
  lần. Câu hỏi phỏng vấn DE nào cũng có, và là trọng tâm của Phase 7 (Airflow).
  Bước 6 của notebook sẽ đo tận mắt: chạy hai lần, đếm dòng.

  Bộ lọc một ngày để chạy nhanh — bài học nằm ở TỶ LỆ dòng nhân lên, không phải
  ở con số tuyệt đối.
*/

select
    cast(tpep_pickup_datetime as timestamp) as pickup_at,
    cast(total_amount as double)            as total_amount
from {{ source('bronze', 'yellow_trips') }}
where date(tpep_pickup_datetime) = '2024-01-15'
