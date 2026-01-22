# Executer un container pyspark avec le script de connexion au S3

```bash
docker exec -it exam-sparkmaster-1 /opt/spark/bin/spark-submit --master spark://sparkmaster:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 /tmp/scripts/connect-s3.py
```
```bash
docker exec -it exam-sparkmaster-1 /opt/spark/bin/spark-submit --master spark://sparkmaster:7077 --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2" --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp/.ivy2" --packages org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.9 /tmp/scripts/connect-s3.py
```
```bash
docker exec -it exam-sparkmaster-1 /opt/spark/bin/spark-submit --master spark://sparkmaster:7077 --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2" --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp/.ivy2" --packages org.apache.hadoop:hadoop-aws:3.4.1,org.postgresql:postgresql:42.7.9 /tmp/scripts/connect-s3.py
```