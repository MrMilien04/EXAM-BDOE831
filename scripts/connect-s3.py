from pyspark.sql import SparkSession

# Configuration des accès MinIO
s3_access_key = "SofpZm1prCGzWUZgOi0M"
s3_secret_key = "sxG0ChkpmMvo3xMi9HW3ib2olkkfYJxKAsLmXNj7"
s3_endpoint = "http://datalake:9010" # "minio" est le nom du service dans docker-compose

spark = SparkSession.builder \
    .appName("Ingestion_MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()