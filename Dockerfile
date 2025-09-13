# Dockerfile
FROM apache/airflow:2.10.5-python3.10

# Cambiar a root para instalar Java y otras dependencias del SO
USER root

# Instalar solo Java 17 (Python ya está disponible en la imagen base)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    gnupg \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Establecer JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Volver al usuario airflow
USER airflow

# Instalar el provider de Spark
RUN pip install apache-airflow-providers-apache-spark==5.0.0 pyspark==3.5.3