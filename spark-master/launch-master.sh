export SPARK_HOME=/opt/spark
export SPARK_MASTER_HOST=`hostname`

. "${SPARK_HOME}/sbin/spark-config.sh"
. "${SPARK_HOME}/bin/load-spark-env.sh"

$SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
    --ip $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_UI_PORT 
