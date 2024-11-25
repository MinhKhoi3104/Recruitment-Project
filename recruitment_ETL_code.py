import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from datetime import date, timedelta

spark = SparkSession.builder.getOrCreate()

#Kết nối với mysql
def read_from_mysql():
    # Tạo SparkSession
    spark = SparkSession.builder \
        .appName("ReadMySQL") \
        .config("spark.jars", "/path/to/mysql-connector-java-8.0.xx.jar") \
        .getOrCreate()
    # Thông tin kết nối MySQL
    jdbc_url = "jdbc:mysql://localhost:3306/recruitment_cassandra_database"
    table_name = "tracking"
    user = "root"  # Thay bằng username của bạn
    password = ""  # Thay bằng password của bạn
    # Đọc dữ liệu từ MySQL
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .load()
    # Hiển thị dữ liệu
    df.show(truncate=False)
    return df

df = df.select('create_time', 'campaign_id', 'bid', 'custom_track', 'group_id', 'job_id', 'publisher_id','uid')