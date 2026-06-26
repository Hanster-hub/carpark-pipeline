FROM apache/airflow:2.9.0
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir mlflow==2.13.0 scikit-learn pandas pyarrow psycopg2-binary sqlalchemy requests
COPY common /opt/airflow/common
COPY ml /opt/airflow/ml
COPY demo_eval.py /opt/airflow/demo_eval.py
