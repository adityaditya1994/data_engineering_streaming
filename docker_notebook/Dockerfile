FROM quay.io/jupyter/pyspark-notebook:spark-3.5.0

USER root

# Install boto3
RUN pip install boto3 pandas

# Download Dependencies (Hadoop AWS + Iceberg)
# Spark 3.5.0 uses Hadoop 3.3.4 by default
ENV SPARK_JARS_DIR=/usr/local/spark/jars

# Hadoop AWS & AWS SDK
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P $SPARK_JARS_DIR/ \
    && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P $SPARK_JARS_DIR/

# Iceberg Runtime for Spark 3.5
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar -P $SPARK_JARS_DIR/ \
    && wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.2/iceberg-aws-bundle-1.4.2.jar -P $SPARK_JARS_DIR/

# Fix permissions
RUN chown -R ${NB_UID} $SPARK_JARS_DIR

USER ${NB_UID}
