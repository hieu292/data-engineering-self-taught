{{ config(materialized = 'table') }}

/*
  BẢNG CHIỀU (dimension). 265 dòng, nhỏ xíu, nhưng đây là mảnh ghép biến
  `PULocationID = 132` thành "JFK Airport, Queens" — nghĩa là biến một con số
  thành một câu trả lời.

  Nguồn của nó là `seed`: một file CSV nằm trong git, dbt nạp thẳng vào kho.
  Seed dành riêng cho dữ liệu tra cứu vừa NHỎ vừa ÍT ĐỔI (bảng mã, ánh xạ
  quốc gia, ngày lễ). Không bao giờ dùng seed cho dữ liệu nghiệp vụ — nó sẽ
  phình ra và biến git thành cơ sở dữ liệu.

  Bảng này nhỏ tới mức Spark sẽ tự broadcast khi join — đúng thứ đã đo ở
  bước 6 Phase 3. 265 dòng không cần shuffle đi đâu cả.
*/

select
    cast(LocationID as int)   as zone_id,
    Borough                   as borough,
    Zone                      as zone_name,
    service_zone              as service_zone
from {{ ref('taxi_zones') }}

/*
  BẪY ĐÃ SẬP MỘT LẦN, ĐỂ LẠI ĐÂY LÀM CHỨNG.

  Bản đầu tiên của model này có thêm dòng `where Borough != 'Unknown'` — nghe
  rất hợp lý: vùng "Unknown" thì lọc đi cho sạch. Bài test `relationships` đỏ
  ngay với **107.092 dòng**: đó là số chuyến có `pickup_zone_id = 264`, chính
  là vùng vừa bị xoá.

  Bài học: LỌC BẢNG CHIỀU LÀ TỰ TẠO RA DÒNG MỒ CÔI. Bảng chiều phải ĐẦY ĐỦ,
  đúng bằng tập giá trị mà bảng sự kiện có thể trỏ tới. Muốn giấu vùng Unknown
  khỏi báo cáo thì lọc ở tầng gold hoặc ở dashboard — không phải ở đây.

  Nếu lọc ở đây mà không có test, hậu quả còn tệ hơn cả mất dòng: `left join`
  ở gold vẫn chạy êm, chỉ là borough thành NULL và 107 nghìn chuyến lặng lẽ
  rơi khỏi mọi con số tổng theo quận. Không ai báo lỗi. Đó là lý do bài test
  `relationships` đáng giá hơn vẻ ngoài buồn tẻ của nó.
*/
