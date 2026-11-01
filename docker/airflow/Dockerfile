# Image unique Airflow + Spark (mode local) : le scheduler lance les jobs avec
# spark-submit dans son propre conteneur. Simple à exécuter en local ; en
# production on soumettrait les jobs à un cluster (voir README).
FROM apache/airflow:2.10.5-python3.11

USER root
# Spark tourne sur la JVM : Java 17 (JRE suffit).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# Versions épinglées : on fige "apache-airflow" pour que pip ne le mette pas à jour.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# JAR alignés sur Spark 3.5 / Hadoop 3.3.4 (version embarquée par pyspark 3.5),
# téléchargés au build et placés dans le classpath de pyspark.
ARG MAVEN=https://repo1.maven.org/maven2
RUN SPARK_JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && cd "${SPARK_JARS}" \
    && curl -fsSLO "${MAVEN}/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar" \
    && curl -fsSLO "${MAVEN}/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar" \
    && curl -fsSLO "${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    && curl -fsSLO "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    && curl -fsSLO "${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
