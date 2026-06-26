FROM apache/spark:3.5.1
WORKDIR /app
USER root
RUN pip install --no-cache-dir psycopg2-binary && mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp/.ivy2
COPY common ./common
COPY streaming ./streaming
ENV PYTHONPATH=/app
USER spark
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--conf", "spark.jars.ivy=/tmp/.ivy2", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", "streaming/stream_job.py"]