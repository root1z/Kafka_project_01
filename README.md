# Kafka Project 01 - Producer đọc data từ source

Dự án Kafka Producer để đọc dữ liệu từ nhiều nguồn khác nhau và gửi đến Kafka.

## Tính năng

- ✅ Đọc từ file CSV, JSON, hoặc text
- ✅ Đọc từ stdin
- ✅ Đọc từ API REST
- ✅ Đọc từ database (PostgreSQL, MySQL, SQLite)
- ✅ Tự động tạo topic nếu chưa tồn tại
- ✅ Hỗ trợ configurable Kafka settings
- ✅ Error handling và logging

## Cài đặt

```bash
# Cài đặt dependencies
pip install confluent-kafka

# Hoặc nếu dùng uv (đã có trong project)
uv sync
```

## Sử dụng

### 1. Đọc từ file CSV

```bash
python producer.py --topic test-topic --file example_data.csv --file-type csv --create-topic
```

### 2. Đọc từ file JSON

```bash
python producer.py --topic json-topic --file example_data.json --file-type json --create-topic
```

### 3. Đọc từ file text (mỗi dòng = 1 message)

```bash
python producer.py --topic text-topic --file data.txt --file-type text --create-topic
```

### 4. Đọc từ stdin

```bash
echo "Hello Kafka" | python producer.py --topic stdin-topic --stdin --create-topic

# Hoặc
cat file.txt | python producer.py --topic stdin-topic --stdin --create-topic
```

### 5. Đọc từ API REST

```bash
python producer.py --topic api-topic --api https://jsonplaceholder.typicode.com/users --create-topic
```

### 6. Đọc từ Database PostgreSQL

```bash
python producer.py --topic db-topic \
  --database "host=localhost dbname=mydb user=user password=pass" \
  --db-query "SELECT * FROM users" \
  --db-type postgresql \
  --create-topic
```

### 7. Với Kafka server khác

```bash
python producer.py \
  --bootstrap-servers kafka-server:9092 \
  --topic my-topic \
  --file data.csv
```

### 8. Tạo topic với nhiều partitions

```bash
python producer.py \
  --topic large-topic \
  --file data.csv \
  --create-topic \
  --partitions 3 \
  --replication-factor 1
```

## Các tham số

### Required:
- `--topic`: Tên topic để gửi message (bắt buộc)

### Source (chọn một):
- `--file`: Đường dẫn đến file
- `--stdin`: Đọc từ stdin
- `--api`: URL của API
- `--database`: Connection string cho database

### Optional:
- `--bootstrap-servers`: Kafka broker address (default: localhost:9092)
- `--create-topic`: Tạo topic nếu chưa tồn tại
- `--partitions`: Số partitions cho topic mới (default: 1)
- `--replication-factor`: Replication factor (default: 1)
- `--file-type`: Loại file - csv/json/text/auto (default: auto)
- `--csv-delimiter`: Delimiter cho CSV (default: ,)
- `--csv-no-header`: File CSV không có header
- `--db-type`: Loại database - postgresql/mysql/sqlite (default: postgresql)
- `--db-query`: SQL query để lấy data (bắt buộc khi dùng --database)
- `--api-method`: HTTP method cho API (default: GET)

## Ví dụ

Xem file `example_usage.sh` để biết thêm các ví dụ sử dụng.

## Ví dụ data

- `example_data.csv`: File CSV mẫu
- `example_data.json`: File JSON mẫu

## Lưu ý

1. Đảm bảo Kafka server đang chạy trước khi chạy producer
2. Với database sources, cần cài thêm thư viện tương ứng:
   - PostgreSQL: `pip install psycopg2-binary`
   - MySQL: `pip install mysql-connector-python`
3. Với API sources, cần: `pip install requests`

