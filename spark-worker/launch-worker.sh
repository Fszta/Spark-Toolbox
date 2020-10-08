export SPARK_HOME=/opt/spark

. "${SPARK_HOME}/sbin/spark-config.sh"
. "${SPARK_HOME}/bin/load-spark-env.sh"

$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER --webui-port $SPARK_WORKER_UI_PORT



