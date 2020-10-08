FROM spark-base:3.0.1

ENV SPARK_MASTER_PORT 7077
ENV SPARK_UI_PORT 8080

EXPOSE 7077 8080

COPY launch-master.sh .

CMD ["/bin/bash","launch-master.sh"]

