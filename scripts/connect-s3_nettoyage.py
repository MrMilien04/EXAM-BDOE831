from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, to_timestamp, unbase64, decode, expr

# Configuration des accès MinIO
s3_access_key = "SofpZm1prCGzWUZgOi0M"
s3_secret_key = "sxG0ChkpmMvo3xMi9HW3ib2olkkfYJxKAsLmXNj7"
s3_endpoint = "http://datalake:9000" # "minio" est le nom du service dans docker-compose

spark = SparkSession.builder \
    .appName("Ingestion_MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1") \
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
    
# Connexion à PostgreSQL
jdbc_url = "jdbc:postgresql://postgresdb:5432/BDOE831"
db_properties = {
    "user": "atelier",
    "password": "superpassword",
    "driver": "org.postgresql.Driver"
}

# 1. Chargement du fichier brut
raw_df = spark.read.text("s3a://datalake/web-access-server-log/access.log")

# 2. Définition du pattern Regex pour parser le format de log
# Groupe 1: IP, Groupe 2: Date, Groupe 3: Méthode, Groupe 4: URL, Groupe 5: Status, Groupe 6: Taille, Groupe 7: User-Agent
log_pattern = r'^(\S+) \S+ \S+ \[(.*?)\] "(.*?) (.*?) \S+" (\d+) (\d+) ".*?" "(.*?)"'

extracted_df = raw_df.select(
    regexp_extract('value', log_pattern, 1).alias('ip'),
    regexp_extract('value', log_pattern, 2).alias('timestamp_raw'),
    regexp_extract('value', log_pattern, 3).alias('method'),
    regexp_extract('value', log_pattern, 4).alias('url'),
    regexp_extract('value', log_pattern, 5).alias('status_code_str'),      
    regexp_extract('value', log_pattern, 6).alias('content_size_str'), 
    regexp_extract('value', log_pattern, 7).alias('user_agent')
)

cleaned_access_df = extracted_df.withColumn(
    "status_code", expr("try_cast(status_code_str as int)")
).withColumn(
    "content_size", expr("try_cast(content_size_str as long)")
).withColumn(
    "timestamp", to_timestamp(col("timestamp_raw"), "dd/MMM/yyyy:HH:mm:ss Z")
).filter(
    col("status_code").isNotNull()
).drop("status_code_str", "content_size_str", "timestamp_raw")

cleaned_access_df.show(5)

# Chargement direct des CSV
df_hostname = spark.read.csv("s3a://datalake/web-access-server-log/client_hostname.csv", header=True, inferSchema=True)
df_net_logs = spark.read.csv("s3a://datalake/intrusion-detection/Network_logs.csv", header=True, inferSchema=True)
df_time_series = spark.read.csv("s3a://datalake/intrusion-detection/Time-Series_Network_logs.csv", header=True, inferSchema=True)

# Envoi des tables vers PostgreSQL
cleaned_access_df.write.jdbc(url=jdbc_url, table="access_logs_cleaned", mode="overwrite", properties=db_properties)
df_hostname.write.jdbc(url=jdbc_url, table="client_hostnames", mode="overwrite", properties=db_properties)
df_net_logs.write.jdbc(url=jdbc_url, table="network_logs", mode="overwrite", properties=db_properties)
df_time_series.write.jdbc(url=jdbc_url, table="time_series_logs", mode="overwrite", properties=db_properties)