-- Load UDF Python
REGISTER '/home/huydq/workspace/chuyende/lab11/convert_temp.py' USING jython AS myudf;

-- Load dữ liệu từ HDFS
records = LOAD 'hdfs://localhost:9000/user/hadoop/input/book_sample.txt'
    USING PigStorage('\t')
    AS (year:chararray, temperature:int, quality:int);

-- Lọc dữ liệu hợp lệ
filtered = FILTER records BY temperature != 9999 AND quality IN (0,1,4,5,9);

-- Gọi hàm UDF để chuyển đổi nhiệt độ sang độ Fahrenheit
converted = FOREACH filtered GENERATE
    year,
    temperature,
    myudf.to_fahrenheit(temperature) AS temp_f;

-- Lưu kết quả
STORE converted INTO 'hdfs://localhost:9000/user/hadoop/output/temp_fahrenheit' USING PigStorage(',');
