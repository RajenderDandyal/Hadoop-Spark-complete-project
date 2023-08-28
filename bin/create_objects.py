
import logging
import logging.config
import findspark
import os
# location = findspark.find()
findspark.init('/opt/spark3', edit_rc=True)
from pyspark.sql import SparkSession
# Load the Logging Configuration File
# logging.config.fileConfig(fname= os.path.join('..','/util/logging_to_file.conf'))
logger = logging.getLogger(__name__)

def get_spark_object(envn,appName ):
    try:
        logger.info(f"get_spark_object() is started. The '{envn}' envn is used.")
        if envn == 'TEST' :
            master='local'
        else:
            master='yarn'
        spark = SparkSession \
                  .builder \
                  .master(master) \
                  .appName(appName) \
                  .getOrCreate()
        logger.info("Spark Object is created ...")
        return spark
    except NameError as exp:
        logger.error("NameError in the method - get_spark_object(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    except Exception as exp:
        logger.error("Error in the method - get_spark_object(). Please check the Stack Trace. " + str(exp),exc_info=True)
    else:
        logger.info("Spark Object is created ...")
    



