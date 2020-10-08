FROM spark-base:3.0.1

ENV SPARK_MASTER "spark://spark-master:7077"
ENV SPARK_WORKER_UI_PORT 8080
COPY launch-worker.sh .

EXPOSE 8080

CMD ["/bin/bash","launch-worker.sh"]
