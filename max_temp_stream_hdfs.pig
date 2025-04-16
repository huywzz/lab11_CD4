DEFINE is_good_quality `is_good_quality.py`
 SHIP ('/home/huydq/workspace/chuyende/lab11/is_good_quality.py');
records = LOAD 'hdfs://localhost:9000/user/hadoop/input/book_sample.txt'
         USING PigStorage('\t')
         AS (year:chararray, temperature:int, quality:int);

-- Truyền qua script Python trên HDFS
filtered_records = STREAM records
    THROUGH is_good_quality
    AS (year:chararray, temperature:int);

grouped = GROUP filtered_records BY year;

max_temp = FOREACH grouped GENERATE group AS year,
           MAX(filtered_records.temperature) AS max_temp;

STORE max_temp INTO '/user/huydq/output/max_temp_filtereds'
USING PigStorage('\t');
