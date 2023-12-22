import sys
from datetime import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook 
import logging


def extract_data(entityset,columnList,keyFigureList,renamedColumnList,Filter,pageSize,gcp_conn_id,gcs_bucket,filename,timePeriodLevel,isCurrency,segmentationColumn,segmentSize):
    from ibpextract.casc_extract_ibp_generator_VF import ExtractIbpDataGen
    import numpy as np

    print(f"we will start the extraction for the table {filename}...")
    logging.info(datetime.now())
    logging.info(f'The parameters for the table {filename} are:')
    logging.info(f'columnList : {columnList}')
    logging.info(f'keyFigureList : {keyFigureList}')
    logging.info(f'renamedColumnList : {renamedColumnList}')
    logging.info(f'filter : {Filter}')
    logging.info(f'pagesize : {pageSize}')
    logging.info(f'filename : {filename}')
    logging.info(f'TimeperiodLevel : {timePeriodLevel}')
    logging.info(f'isCurrency : {isCurrency}')
    logging.info(f'segmentationColumn  :  {segmentationColumn}')
    logging.info(f'segmentSize  :  {segmentSize}')
    
#   logging.info(f"the executed function is as follows: ExtractIbpData(entitySet=entitySet,columnList={columnList},keyFigureList={keyFigureList},renamedColumnList={renamedColumnList},filter={Filter},pageSize={pageSize},timePeriodLevel={timePeriodLevel},isCurrency={isCurrency})")

    try:
        Generator=ExtractIbpDataGen(entitySet=entityset,columnList=columnList,keyFigureList=keyFigureList,renamedColumnList=renamedColumnList,filter=Filter,pageSize=pageSize,timePeriodLevel=timePeriodLevel,isCurrency=isCurrency,segmentationColumn=segmentationColumn,segmentSize=segmentSize)
        logging.info(f'The start time of the extraction is {datetime.now()}')
        df_header=True
        df_mode='w'
        sequence = 1
        for dataFrame in Generator:
            dataFrame.replace('', np.nan, inplace=True)
            csvfile= dataFrame.to_csv(sep='|', index=False, mode=df_mode, header=df_header)
            hook = GCSHook(gcp_conn_id=gcp_conn_id)
            hook.upload(
                bucket_name=gcs_bucket,
                object_name=f"{filename}_{sequence}.csv",
                data=csvfile.encode('utf-8'),
                mime_type='text/csv',
                timeout=(10, 700)
                )
            sequence += 1
        logging.info(f"the extraction of the table {filename} is done ")
        logging.info('\n\n\n')
    except:
        logging.exception("An error occurred while extracting data from the Odata")
        sys.exit()
    logging.info(f"the extraction of the table {filename} is done ")
    logging.info('\n\n\n')
    


#Define a SubDag to be executed for every Table from the dataframe generated on top of Cloud Sql data (IBPExtractParameters Table)
