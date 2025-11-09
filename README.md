## Chạy pipeline: gửi từ sample vào Kafka rồi consume ghi Mongo
Cấu trúc thư mục:
```
~/Kafka_project_01/
- .gitignore
- .python-version
- README.md
- config/
  - kafka_config.py
  - mongo_config.py
- consumer/
  - kafka_consumer.py
- docker-compose.yml
- main.py
- producer/
  - kafka_producer.py
- pyproject.toml
- sample_data.json
- utils/
  - logger.py
- uv.lock
```

1) Cài đặt phụ thuộc và cấu hình `.env` sử dụng uv.

2) Khởi động MongoDB bằng Docker Compose nếu dùng Docker:

```
docker compose up -d
```

3) Từ thư mục dự án, chạy main.py:

``` 
python main.py
```
- Lệnh trên sẽ:
  - Đảm bảo topic đầu vào tồn tại (tạo nếu thiếu).
  - Đọc `sample_data.json` và gửi lên topic đầu vào.
  - Chạy consumer để poll dữ liệu từ Kafka và ghi vào MongoDB các collection:
    - `data_messages`
    - `error_messages`
    - `processed_offsets`