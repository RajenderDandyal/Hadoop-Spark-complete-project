# Call Delete HDFS paths
printf "Calling delete_hdfs_output_path.ksh..."
/home/rajender/Downloads/pysparkTutorial/bin/delete_hdfs_output_path.ksh
printf "Executed delete_hdfs_output_path.ksh..."

printf "Calling run_presc_pipeline.py..."
spark-submit --master yarn --num-executors 28 run_presc_pipeline.py
printf "Executed run_presc_pipeline.py..."
