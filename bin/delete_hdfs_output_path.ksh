CITY_PATH=PrescPipeline/output/dimension_city
hdfs dfs -test -d $CITY_PATH
status=$?
if [ $status == 0 ]
  then
  echo "The HDFS output directory $CITY_PATH is available. Proceed to delete."
  hdfs dfs -rm -r -f $CITY_PATH
  echo "The HDFS output directory $CITY_PATH is deleted before extraction."
fi


FACT_PATH=PrescPipeline/output/fact
hdfs dfs -test -d $FACT_PATH
status=$?
if [ $status == 0 ]
  then
  echo "The HDFS output directory $FACT_PATH is available. Proceed to delete."
  hdfs dfs -rm -r -f $FACT_PATH
  echo "The HDFS output directory $FACT_PATH is deleted before extraction."
fi
