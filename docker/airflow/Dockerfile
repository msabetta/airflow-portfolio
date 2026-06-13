FROM apache/airflow:latest

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

COPY plugins /opt/airflow/plugins

COPY dags /opt/airflow/dags

COPY include /opt/airflow/include

COPY config /opt/airflow/config

RUN mkdir -p /opt/airflow/include/data

RUN mkdir -p /opt/airflow/include/models

RUN mkdir -p /opt/airflow/dags

COPY include/templates /opt/airflow/include/templates

COPY dags /opt/airflow/dags

COPY include/data /opt/airflow/include/data

COPY include/models /opt/airflow/include/models

COPY config/airflow.cfg /opt/airflow/config/airflow.cfg

CMD ["bash"]
