FROM alpine:3.12

ENV HADOOP_VERSION=3.2
ENV SPARK_VERSION=3.0.1
ENV SPARK_HOME=/opt/spark

RUN apk add --no-cache bash openjdk8-jre libc6-compat \
  && ln -s /lib64/ld-linux-x86-64.so.2 /lib/ld-linux-x86-64.so.2 \
  && wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
  && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
  && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
  && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
