FROM apache/airflow:2.7.3

USER root

# 1. Installer Java et curl
RUN apt-get update && apt-get install -y openjdk-11-jdk curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/lib/jvm/java-11-openjdk-amd64/bin/java /usr/local/bin/java

# 2. Télécharger et installer Spark (avec logs et repli)
RUN mkdir -p /usr/local/spark && \
    echo "=== Téléchargement de Spark ===" && \
    curl -fSL --insecure https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o /tmp/spark.tgz || \
      (echo "⚠️ Premier miroir échoué, tentative depuis archive.apache.org" && \
       curl -fSL --insecure https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o /tmp/spark.tgz) && \
    echo "=== Vérification du fichier ===" && \
    ls -lh /tmp/spark.tgz && \
    echo "=== Extraction de Spark ===" && \
    tar -xzf /tmp/spark.tgz -C /usr/local/spark --strip-components=1 && \
    rm /tmp/spark.tgz

# 3. Variables d’environnement
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

# 4. Installer les dépendances Python
COPY requirements.txt /requirements.txt
USER airflow
RUN pip install --upgrade pip && pip install -r /requirements.txt
