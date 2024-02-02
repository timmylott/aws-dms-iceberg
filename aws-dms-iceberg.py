# Retrieve job configuration from Parameter Store
def getConfigFromSSM(SSM_PARAMETER_NAME):
    ssm_param_values = json.loads(ssmClient.get_parameter(Name = SSM_PARAMETER_NAME)['Parameter']['Value'])

    res = [ssm_param_values['rawBucket'], ssm_param_values['rawBucketPrefix'], ssm_param_values['stageS3BucketName'], ssm_param_values['warehouses3Path']]
    return res

# Retrieve table configuration from JSON content retrieved from Parameter Store
def getTableInfoFromSSM(ssm_param_table_values_key):
    primary_condition = ' '
    primaryKey = ssm_param_table_values_key['primaryKey']
    dbName = ssm_param_table_values_key['domain']
    keylist = primaryKey.split(',')
    for key in  keylist:
        primary_condition += f'target.{key}=source.{key} and '
    primary_key_condition = primary_condition[ : -5]
    partitionCols = ssm_param_table_values_key.get('partitionCols', ' ')
    partitionStr = ""
    partitionStrSQL = ""
    if partitionCols != '':
        partitionStr = f'PARTITIONED BY({partitionCols})'
        partitionStrSQL = f'ORDER BY {partitionCols}'

    res = [primaryKey, partitionCols, dbName, keylist, primary_key_condition, partitionStr, partitionStrSQL]
    return res

# Read incoming data from Amazon S3
def readS3DF(rawS3BucketName, rawBucketPrefix, schemaName, tableName,source_database_name):
    inputDf = glueContext.create_dynamic_frame_from_options(
        connection_type = 's3', 
        connection_options = {
            'paths': [f's3://{rawS3BucketName}/{rawBucketPrefix}/{source_database_name}/{schemaName}/{tableName}'], 
            'groupFiles': 'none', 
            'recurse':True
        }, 
        format = 'parquet',
        transformation_ctx = f'{tableName}'
    ).toDF()

    return inputDf

## Apply De-duplication logic on input data, to pickup latest record based on timestamp and operation 
def dedupCDCRecords(inputDf, keylist):
    IDWindowDF = Window.partitionBy(*keylist).orderBy(inputDf.last_update_time).rangeBetween(-sys.maxsize, sys.maxsize)
    inputDFWithTS = inputDf.withColumn('max_op_date', max(inputDf.last_update_time).over(IDWindowDF))
    
    NewInsertsDF = inputDFWithTS.filter('last_update_time=max_op_date').filter("op='I'")
    UpdateDeleteDf = inputDFWithTS.filter('last_update_time=max_op_date').filter("op IN ('U','D')")
    finalInputDF = NewInsertsDF.unionAll(UpdateDeleteDf)

    return finalInputDF

# Create database on the AWS Glue Data Catalog
def createDatabaseSparkSQL(dbName, stageS3BucketName, rawS3BucketName):
    sqltemp = Template("""
        CREATE DATABASE IF NOT EXISTS $dbName LOCATION 's3://$rawS3BucketName/$stageS3BucketName/$dbName'
    """)
    print(rawS3BucketName)
    print(stageS3BucketName)
    print(dbName)
    SQLQUERY = sqltemp.substitute(
        dbName = dbName, 
        stageS3BucketName = stageS3BucketName,
        rawS3BucketName = rawS3BucketName)
    logger.info(f'****SQL QUERY IS : {SQLQUERY}')
    spark.sql(SQLQUERY)

# Create table on the AWS Glue Data Catalog
def createTableSparkSQL(stageS3BucketName, dbName, tableName, tableColumns, partitionStrSQL, rawS3BucketName, schemaName):
    targetPath = f's3://{rawS3BucketName}/{stageS3BucketName}/{dbName}/{schemaName}_{tableName.lower()}'
    inputDfWithoutControlColumns.createOrReplaceTempView('appendTable')
    logger.info('***** Creating table and inserting initial data')
    sqltemp = Template("""
        CREATE TABLE $catalog_name.$dbName.$tableName $partitionStr LOCATION '$targetPath' as SELECT $tableColumns FROM appendTable where 1=0 $partitionStrSQL
    """)
    SQLQUERY = sqltemp.substitute(
        catalog_name = catalog_name, 
        dbName = dbName, 
        tableName = f'{schemaName}_{tableName.lower()}', 
        partitionStr = partitionStr,
        targetPath = targetPath,
        tableColumns = tableColumns, 
        partitionStrSQL = partitionStrSQL)
    logger.info(f'****SQL QUERY IS : {SQLQUERY}')
    spark.sql(SQLQUERY)

# Merge incoming changes into the Iceberg table
def upsertRecordsSparkSQL(finalInputDF, inputDfWithoutControlColumns_columns, schemaName):
    finalInputDF.createOrReplaceTempView('upsertTable')
    updateTableColumnList = ''
    insertTableColumnList = ''
    for column in inputDfWithoutControlColumns_columns:
        updateTableColumnList += f" target.{column} = source.{column},"
        insertTableColumnList += f" source.{column},"

    logger.info('***** Upserting data')
    sqltemp = Template("""
        MERGE INTO $catalog_name.$dbName.$tableName target
        USING (SELECT * FROM upsertTable $partitionStrSQL) source
        ON $primary_key_condition
        WHEN MATCHED AND source.Op = 'D' THEN DELETE
        WHEN MATCHED AND source.Op in ('U', 'I') THEN UPDATE SET $updateTableColumnList
        WHEN NOT MATCHED and source.Op = 'I' THEN INSERT ($tableColumns) values ($insertTableColumnList)
    """)
    SQLQUERY = sqltemp.substitute(
        catalog_name = catalog_name, 
        dbName = dbName, 
        tableName = f'{schemaName}_{tableName.lower()}', 
        partitionStrSQL = partitionStrSQL, 
        primary_key_condition = primary_key_condition, 
        updateTableColumnList = updateTableColumnList[ : -1], 
        tableColumns = tableColumns, 
        insertTableColumnList = insertTableColumnList[ : -1])

    logger.info(f'****SQL QUERY IS : {SQLQUERY}')
    spark.sql(SQLQUERY)

# Perform initial data loading into an empty Iceberg table
def initialLoadRecordsSparkSQL(finalInputDF, inputDfWithoutControlColumns_columns, schemaName):
    finalInputDF.createOrReplaceTempView('insertTable')
    insertTableColumnList = ''
    for column in inputDfWithoutControlColumns_columns:
        insertTableColumnList += f" {column},"

    logger.info('***** Inserting initial data')
    sqltemp = Template("""
        INSERT INTO $catalog_name.$dbName.$tableName  ($insertTableColumnList)
        SELECT $insertTableColumnList FROM insertTable $partitionStrSQL
    """)
    SQLQUERY = sqltemp.substitute(
        catalog_name = catalog_name, 
        dbName = dbName, 
        tableName = f'{schemaName}_{tableName.lower()}',
        insertTableColumnList = insertTableColumnList[ : -1],
        partitionStrSQL = partitionStrSQL)

    logger.info(f'****SQL QUERY IS : {SQLQUERY}')
    spark.sql(SQLQUERY)

# Main application
import sys
import os
import json
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import max, lit
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
from string import Template

glueClient = boto3.client('glue')
ssmClient = boto3.client('ssm')

## Parameters for job
#args = getResolvedOptions(sys.argv, ['JOB_NAME', 'stackName'])
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#SSM_PARAMETER_NAME = f"{args['stackName']}-iceberg-config"
#SSM_TABLE_PARAMETER_NAME = f"{args['stackName']}-iceberg-tables"
#rawS3BucketName, rawBucketPrefix, stageS3BucketName, warehouse_path = getConfigFromSSM(SSM_PARAMETER_NAME)
rawS3BucketName = "mylott-test"
rawBucketPrefix = "dms-raw"
stageS3BucketName = "dms-stage"
warehouse_path = f"s3://{rawS3BucketName}/iceberg_warehouse"
source_database_name = 'triumphpay'

#ssm_param_table_values = json.loads(ssmClient.get_parameter(Name = SSM_TABLE_PARAMETER_NAME)['Parameter']['Value'])

ssm_param_table_values = json.loads(""" 
{
    "audit.AuditLogDetail": {
        "primaryKey": "AuditLogDetailId",
        "domain": "triumphpay_iceberg",
        "partitionCols": ""
    },
    "audit.EventLog": {
        "primaryKey": "EventLogId",
        "domain": "triumphpay_iceberg",
        "partitionCols": ""
    },
    "dbo.PayeeRelationship": {
        "primaryKey": "PayeeRelationshipId",
        "domain": "triumphpay_iceberg",
        "partitionCols": ""
    }                                    
}
""")

dropColumnList = ['db','table_name', 'schema_name','Op', 'last_update_time', 'max_op_date']
catalog_name = 'AwsDataCatalog'
dynamodb_table = "iceberg_table_lock_mylott_test_s3_iceberg"
errored_table_list = []

## Iceberg configuration
spark = SparkSession.builder \
    .config('spark.sql.warehouse.dir', warehouse_path) \
    .config(f'spark.sql.catalog.{catalog_name}', 'org.apache.iceberg.spark.SparkCatalog') \
    .config(f'spark.sql.catalog.{catalog_name}.warehouse', warehouse_path) \
    .config(f'spark.sql.catalog.{catalog_name}.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog') \
    .config(f'spark.sql.catalog.{catalog_name}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO') \
    .config(f'spark.sql.catalog.{catalog_name}.lock-impl', 'org.apache.iceberg.aws.glue.DynamoLockManager') \
    .config(f'spark.sql.catalog.{catalog_name}.lock.table', dynamodb_table) \
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

# Iteration over tables stored on Parameter Store
for key in ssm_param_table_values:
    # Get table data
    isTableExists = False
    schemaName, tableName = key.split('.')
    logger.info(f'Processing table : {tableName}')
    try:
        primaryKey, partitionCols, dbName, keylist, primary_key_condition, partitionStr, partitionStrSQL = getTableInfoFromSSM(ssm_param_table_values[key])
    except KeyError as e:
        raise Exception(f'***** Primary key for {tableName} not found in parameter, it is required.')
    
    # Create database if not exists
    try:
        glueClient.get_database(Name=dbName)
    except:
        createDatabaseSparkSQL(dbName, stageS3BucketName, rawS3BucketName)

    try:
        glueClient.get_table(DatabaseName = dbName, Name = f'{schemaName}_{tableName}')
        isTableExists = True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            logger.info(f'***** {dbName}.{tableName} does not exist. Table will be created.')

    ## Read changes from raw bucket
    inputDf = readS3DF(rawS3BucketName, rawBucketPrefix, schemaName, tableName, source_database_name)

    if(inputDf.first() == None):
        logger.info('Dataframe is empty')
        continue;
    else:
        
        if('Op' not in inputDf.columns):
            inputDf.withColumn('Op', lit('I'))

        inputDf = inputDf.na.fill({'Op':'I'})
        if('Op' in inputDf.columns):
            # Dedup incoming changes
            finalInputDF = dedupCDCRecords(inputDf, keylist)
        else:
            finalInputDF = inputDf

        inputDfWithoutControlColumns = finalInputDF.drop(*dropColumnList)
        tableColumns = ','.join(inputDfWithoutControlColumns.columns)
        try:
            if(not isTableExists):
                ## Create table if not exists
                createTableSparkSQL(stageS3BucketName, dbName, tableName, tableColumns, partitionStrSQL, rawS3BucketName, schemaName)

            if('Op' in inputDf.columns):
                # Upsert changes
                upsertRecordsSparkSQL(finalInputDF, inputDfWithoutControlColumns.columns, schemaName)
            else:  
                # Perform initial data loading
                initialLoadRecordsSparkSQL(finalInputDF, inputDfWithoutControlColumns.columns, schemaName)
        
        except Exception as e:
            # Log errors and save table into error array            
            logger.info(f'There is an issue with table: {tableName}')
            logger.info(f'The exception is : {e}')
            errored_table_list.append(tableName)
            continue
job.commit()        

# Verify if errors exists, log them and fail the job
if (len(errored_table_list)):
    logger.info('Total number of errored tables are ',len(errored_table_list))
    logger.info('Tables that failed during processing are ', *errored_table_list, sep=', ')
    raise Exception(f'***** Some tables failed to process.')
