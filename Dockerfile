FROM apache/airflow:latest-python3.12

USER root

# Actualizar los repositorios e instalar Java, Ant y Python 3.8
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Configurar JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install boto3 pyspark pandas