/*
  TEST ĐƠN LẺ (singular test) — chỉ là một câu SQL, quy ước duy nhất:
  **trả về 0 dòng là ĐẬU, trả về dòng nào là RỚT.**
  Khác test kiểu `unique`/`not_null` ở chỗ nó diễn đạt được logic tuỳ ý.

  Đây là bài test đáng giá nhất trong dự án: ĐỐI CHIẾU. Tổng doanh thu ở gold
  phải bằng tổng doanh thu ở silver. Nếu một ngày nào đó ai đó thêm một mệnh đề
  join sai và làm nhân đôi dòng, mọi test not_null/unique vẫn xanh mướt —
  chỉ bài test này đỏ.

  "Số tổng ở tầng trên phải khớp tầng dưới" là dạng test mà kiểm toán viên và
  dân tài chính đòi đầu tiên. Biết viết nó là khác biệt giữa "chạy được dbt"
  và "tin được kết quả".
*/

with silver_total as (
    select sum(total_amount) as amount from {{ ref('silver_trips') }}
),

gold_total as (
    select sum(revenue) as amount from {{ ref('gold_daily_zone_revenue') }}
)

select
    silver_total.amount as silver_amount,
    gold_total.amount   as gold_amount,
    abs(silver_total.amount - gold_total.amount) as gap
from silver_total, gold_total
-- Ngưỡng 1 đô cho sai số làm tròn của kiểu double, không phải để "nới tay".
where abs(silver_total.amount - gold_total.amount) > 1
