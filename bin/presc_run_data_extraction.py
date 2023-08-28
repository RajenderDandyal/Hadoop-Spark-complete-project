import logging

logger = logging.getLogger(__name__)

def extract_files(df, format, filePath, split_no, headerReq, compressionType):
    try:
        logger.info("Extraction - extract_files() is started...")
        df.coalesce(split_no).write.format(format).save(filePath,header=headerReq,compression=compressionType)
    except Exception as exp:
        logger.error("Error in method - extract_files(), Please check the stack trace. " + str(exp), exc_info=True)
    else:
        logger.info("Extraction - extract_files() method is completed ...")