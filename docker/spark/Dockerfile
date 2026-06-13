FROM apache/spark:latest

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

COPY include/scripts /opt/airflow/include/scripts

RUN mkdir -p /opt/airflow/include/data

RUN mkdir -p /opt/airflow/include/models

RUN mkdir -p /opt/airflow/dags

COPY config/spark-defaults.conf /opt/spark/conf/spark-defaults.conf 

COPY include/templates /opt/airflow/include/templates

COPY dags /opt/airflow/dags

COPY include/data /opt/airflow/include/data

COPY include/models /opt/airflow/include/models

CMD ["bash"]
