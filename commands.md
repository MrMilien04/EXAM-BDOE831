# Executer un container pyspark avec le script de connexion au S3
```bash
docker exec -it exam-sparkmaster-1 /opt/spark/bin/spark-submit --master spark://sparkmaster:7077 --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2" --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp/.ivy2" --packages org.apache.hadoop:hadoop-aws:3.4.1,org.postgresql:postgresql:42.7.9 /tmp/scripts/connect-s3.py
```

# Connexion S3
```python
from pyspark.sql import SparkSession

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
    
s3_bucket_path = "s3a://datalake/intrusion-detection/Network_logs.csv"
# Read CSV data from S3 into a DataFrame
df = spark.read.csv(s3_bucket_path, header=True, inferSchema=True)
# Show data
df.printSchema()
```
# Executer un container spark-submit pour se connecter au S3 traiter les données avec pyspark et les importer dans postgresql
```bash
docker exec -it exam-sparkmaster-1 /opt/spark/bin/spark-submit --master spark://sparkmaster:7077 --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2" --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp/.ivy2" --packages org.apache.hadoop:hadoop-aws:3.4.1,org.postgresql:postgresql:42.7.9 /tmp/scripts/connect-s3_nettoyage.py
```