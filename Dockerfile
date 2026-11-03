# Airflow image: DAGs + operator providers only. No data-processing code lives here;
# that is the separate csv-processor image.
FROM apache/airflow:3.1.0-python3.12
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
COPY --chown=airflow:root dags/ /opt/airflow/dags/
