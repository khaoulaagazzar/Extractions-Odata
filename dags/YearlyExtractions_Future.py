from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.models import DAG
from airflow.hooks.base import BaseHook
import pandas as pd
from ibpextract.db_configuration_store import DBConfigurationStore
import logging
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator


import os
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


try:
    current_date = datetime.now()
    

# Format the previous month 
    nextyear = current_date + relativedelta(years=1)
    formatted_year = nextyear.replace(day=1,month=1).strftime("%Y-%m-%d 00:00:00+00:00")

except:
    logging.info('cant generate the current month')

connection = BaseHook.get_connection("cloudsql_db_config")

DB_CONFIG = {
    'server': connection.host,
    'database': 'ETLMetadata',
    'username': connection.login,
    'password': connection.password,
    'driver': 'ODBC Driver 17 for My Sql',
    'ext_props': 'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;',
    'dialect': "mysql+mysqlconnector"
}

logging.info('credentials fetched correctly')
db_conf_store=DBConfigurationStore(DB_CONFIG)
query="""select * from IBPExtractParametersV3 where Frequency="Yearly" and tableType="FACT" ;"""
parameters=db_conf_store.get_data(query)
df=pd.DataFrame(parameters,columns=['SourceDomain','SourceName','TargetName','SourceColumns','isKeyFigure','ColumnToRename','RenamedColumns','Filter','PageSize','TimePeriodLevel','isCurrency','segmentationColumn','segmentSize','segmentPeriod','tableType','Frequency'])
print(df)
if not isinstance(df, pd.DataFrame):
    raise Exception('Data frame not generated!')
    sys.exit()
    
else:
    logging.info('dataframe generated. the metadata is transformed to dataframe')
    #delete nan values in the dataframe and replace it with blank values ''
    df.fillna('',inplace=True)

def create_subdag(parent_dag_id,child_dag_id,args,TargetName,dataframe):
    from ibpextract.casc_extract_ibp_generator_VF import create_session,ExtractIbpDataGen
    import ibpextract.VARIABLES as VARIABLES
    from ibpextract.mainV3 import extract_data
    subdag=DAG(
        dag_id=f'{parent_dag_id}.{child_dag_id}',
        schedule_interval=args['schedule_interval'],
        start_date=args['start_date'],
        max_active_tasks=11
    )
    url=VARIABLES.SERVICE_URL
    user_id=VARIABLES.USER_ID
    password=VARIABLES.PASSWORD

    with subdag:

        entityset=create_session(url,user_id,password)
        ##########################################PARAMETERS#############################################
        group_df=dataframe[dataframe['TargetName']==TargetName]
        filename=group_df['TargetName'].iloc[0]
        columnList=group_df['SourceColumns'].tolist()
        keyFigureList=group_df.loc[group_df['isKeyFigure'] == 1, 'SourceColumns'].tolist()
        renamedColumnList=group_df.loc[group_df['ColumnToRename']==1,['SourceColumns','RenamedColumns']].set_index('SourceColumns')['RenamedColumns'].to_dict()
        Filter=VARIABLES.Default_values.get('Filter') if not group_df['Filter'].iloc[0] else group_df['Filter'].iloc[0]
        pageSize=round(VARIABLES.Default_values.get('pageSize')) if group_df['PageSize'].iloc[0]==0 else round(group_df['PageSize'].iloc[0])
        timePeriodLevel=round(VARIABLES.Default_values.get('TimePeriodLevel')) if group_df['TimePeriodLevel'].iloc[0]==0 else round(group_df['TimePeriodLevel'].iloc[0])
        isCurrency=False if group_df['isCurrency'].iloc[0]=='False' else True
        segmentationColumn=VARIABLES.Default_values.get('segmentationColumn') if not group_df['segmentationColumn'].iloc[0] else group_df['segmentationColumn'].iloc[0]
        segmentSize=round(VARIABLES.Default_values.get('segmentSize')) if not group_df['segmentSize'].iloc[0] else round(group_df['segmentSize'].iloc[0])
        
        ##################################################################################################
        logging.info(f'the filename to extract is: {filename}')
        task_id=f'Extract_{filename}'

        PeriodExtractGen=ExtractIbpDataGen(entityset,columnList=['PERIODID1_TSTAMP'])

        DfPeriods=next(PeriodExtractGen)
        filtredDf= DfPeriods.loc[DfPeriods['PERIODID1_TSTAMP'].apply(lambda x: x >= formatted_year)]
        PeriodValues=filtredDf.iloc[:, 0]

        
        for elem in PeriodValues:
            indice = f"{elem.year}{elem.month_name()[:3]}"
            elem=elem.strftime('%Y-%m-%dT%H:%M:%S')
            if Filter is not None:
                Filter+=" and PERIODID1_TSTAMP" + " eq " + "datetime'" + f"{elem}" + "' "
            else:
                Filter= " PERIODID1_TSTAMP eq " + "datetime'" + f"{elem}" + "'"
                
            execute_task=PythonOperator(
                task_id=f"{task_id}_{indice}",
                python_callable=extract_data,
                provide_context=True,
                op_kwargs={
                'entityset': entityset,
                'columnList': columnList,
                'keyFigureList' : keyFigureList,
                'renamedColumnList':renamedColumnList,
                'Filter' : Filter,
                'pageSize' : pageSize,
                'timePeriodLevel': timePeriodLevel,
                'isCurrency':isCurrency,
                'segmentationColumn': segmentationColumn,
                'segmentSize':segmentSize,
                'gcp_conn_id' : VARIABLES.GCP_CONNECTION,
                'gcs_bucket' : os.getenv('GCS_BUCKET_LANDING_RAW'),
                'filename' : f"FUTURE_{filename}_{indice}"
                }
                )
            Filter=VARIABLES.Default_values.get('Filter') if not group_df['Filter'].iloc[0] else group_df['Filter'].iloc[0]
            
        

    return subdag

with DAG(
    dag_id='IBP_Yearly_Future_DAG',
    schedule_interval='0 0 1 12 *',
    start_date=datetime(2023, 5, 20),
    max_active_tasks=2,
    catchup=False
) as dag:
    for TargetName in df['TargetName'].unique():
        taskname=TargetName
        executesubdag=SubDagOperator(
        task_id=f'extracting_{taskname}',
        subdag=create_subdag('IBP_Yearly_Future_DAG',f'extracting_{taskname}' , {'schedule_interval': None, 'start_date': datetime(2023, 5, 20)},TargetName,dataframe=df),
    dag=dag
    )
        

