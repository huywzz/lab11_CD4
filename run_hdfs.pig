-- Bước 1: Tải dữ liệu từ HDFS
records = LOAD 'hdfs://localhost:9000/user/hadoop/input/book_sample.txt' 
    USING PigStorage('\t') 
    AS (year:chararray, temperature:int, quality:int);

-- Bước 2: Lọc các bản ghi với nhiệt độ hợp lệ (temperature != 9999) và chất lượng hợp lệ
filtered_records = FILTER records BY temperature != 9999 AND quality IN (0, 1, 4, 5, 9);

-- Bước 3: Nhóm theo năm
grouped_records = GROUP filtered_records BY year;

-- Bước 4: Tính nhiệt độ tối đa cho mỗi nhóm (theo năm)
max_temp = FOREACH grouped_records GENERATE group AS year, MAX(filtered_records.temperature) AS max_temperature;

-- Bước 5: Lưu kết quả vào HDFS
STORE max_temp INTO 'hdfs://localhost:9000/user/hadoop/output/max_temp' USING PigStorage(',');
