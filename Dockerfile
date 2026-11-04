FROM apache/airflow:3.0.3

# Create ~/.kube before it's used as a mount target.
# Without this, Docker creates it as a root-owned directory on first mount.
USER root
RUN mkdir -p /home/airflow/.kube && chown 50000:0 /home/airflow/.kube

USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-cncf-kubernetes