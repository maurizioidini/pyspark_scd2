import boto3
import os
import sys
from datetime import datetime, timezone
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import row_number, col, coalesce, lit, when
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# get parameters from conf
args = getResolvedOptions(sys.argv,
                           [
                            'SourceBucketName',
                            'TargetSchemaName',
                            'TargetTableName',
                            'ClusterIdentifier',
                            'DbUser',
                            'DbName',
                            'RedshiftTmpDir',
                            'DriverJdbc',
                            'Host',
                            'Port',
                            'Region',
                            'Mode',
                            'JobRole',
                            'Pkey'
                           ])

SourceBucketName   = args['SourceBucketName']
target_schema_name = args['TargetSchemaName']
target_table_name  = args['TargetTableName']
cluster_identifier = args['ClusterIdentifier']
db_user            = args['DbUser']
db_name            = args['DbName']
redshift_tmp_dir   = args['RedshiftTmpDir']
driver_jdbc        = args['DriverJdbc']
host               = args['Host']
port               = args['Port']
region             = args['Region']
mode               = args['Mode']
job_role           = args['JobRole']
p_key              = args['Pkey'].split(",")


# create spark context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


redshift_client = boto3.client('redshift', region_name=region)
s3_client = boto3.client('s3')

response_redshift = redshift_client.get_cluster_credentials(
    ClusterIdentifier=cluster_identifier,
    DbUser=db_user,
    DbName=db_name,
    DurationSeconds=3600
)

my_conn_options = {
    "url": f"{driver_jdbc}://{host}:{port}/{db_name}",
    "user": response_redshift['DbUser'],
    "password": response_redshift['DbPassword'],
    "redshiftTmpDir": redshift_tmp_dir,
}


###### DEFINE USEFUL FUNCTIONS

def read_redshift(table):
    query = f"select * from {table}"
    print(f"query: {query}")

    data = (
        spark.read.format("io.github.spark_redshift_community.spark.redshift")
        .option("url", my_conn_options["url"])
        .option("query", query)
        .option("user", response_redshift['DbUser'])
        .option("password", response_redshift['DbPassword'])
        .option("tempdir", my_conn_options["redshiftTmpDir"])
        .option("forward_spark_s3_credentials", "true")
        .option("datetimeRebaseMode", "LEGACY")
        .load()
    )

    return data

def get_last_insertion_date(table_name) -> datetime:

    query = f"select max(ingestion_date) from {table_name}"
    data = (
        spark.read.format("io.github.spark_redshift_community.spark.redshift")
        .option("url", my_conn_options["url"])
        .option("query", query)
        .option("user", response_redshift['DbUser'])
        .option("password", response_redshift['DbPassword'])
        .option("tempdir", my_conn_options["redshiftTmpDir"])
        .option("forward_spark_s3_credentials", "true")
        .option("datetimeRebaseMode", "LEGACY")
        .load()
    )
    last_insertion_date = data.select(data.columns[0]).first()[0]
    if last_insertion_date is None:
        last_insertion_date = datetime(1970,1,1,0,0)
    return last_insertion_date.replace(tzinfo=timezone.utc)


def save_redshift(self, table_name):

    preaction = f"truncate table {table_name}"
    # self.coalesce(10).write \
    #     .format("jdbc") \
    #     .option("url", my_conn_options['url']) \
    #     .option("user", my_conn_options['user']) \
    #     .option("password", my_conn_options['password']) \
    #     .option("dbtable", table_name) \
    #     .option("tempdir", redshift_tmp_dir) \
    #     .option("inferSchema", "true") \
    #     .option("preactions", preaction) \
    #     .option("postactions", "") \
    #     .option("forward_spark_s3_credentials", "true") \
    #     .option("batchsize", 1000) \
    #     .option("maxConnections", 10)\
    #     .mode("append") \
    #     .save()
    print("repartition and cache")
    self.repartition(96).cache()
    print("create dynamic frame")
    data = DynamicFrame.fromDF(self, glueContext, table_name)

    connection_options = my_conn_options | {"dbtable": table_name, "preactions": preaction, "batchmode": "on"}
    print("start writing")
    glueContext.write_dynamic_frame.from_options(
        frame=data,
        connection_type="redshift",
        connection_options = connection_options
    )
    print("finish writing")

    return self

setattr(DataFrame, "save_redshift", save_redshift)


def compute_scd2(self, incoming_df, *p_key):
    print("ENTERING SCD2")
    key_list = list(p_key)

    current_timestamp = F.lit(datetime.now()).cast(TimestampType())
    print("JOIN CONDITION")
    # Costruzione della condizione di join
    join_condition = None
    for key in key_list:
        condition = F.col(f"current.{key}") == F.col(f"incoming.{key}")
        join_condition = condition if join_condition is None else join_condition & condition
    print("OTHER CONDITION")
    # Rilevazione delle modifiche
    condition = None
    for col in incoming_df.columns:
        if col not in key_list:
            if condition is None:
                condition = F.col(f"current.{col}") != F.col(f"incoming.{col}")
            else:
                condition = condition | (F.col(f"current.{col}") != F.col(f"incoming.{col}"))
    print("cache")
    self.cache()
    print("coalesce")
    incoming_df = incoming_df.coalesce(12)
    print("cache")
    incoming_df.cache()

    print("start changes detected")
    # Trovare i record modificati
    changes_detected = incoming_df.alias("incoming") \
                                .join(self.alias("current"), join_condition, "left_outer") \
                                .where(F.coalesce(condition, F.lit(False)))
    print("cache changedetected")
    changes_detected.cache()

    print("start inactive records")
    # Marcare i record vecchi come inattivi
    inactive_records = changes_detected.withColumn("end_validity_date", current_timestamp) \
                                       .select("current.*", "end_validity_date")

    print("start new records")
    # Preparare i nuovi record
    new_records = changes_detected.withColumn("ingestion_date", current_timestamp) \
                                  .withColumn("end_validity_date", F.lit(None).cast(TimestampType())) \
                                  .select("incoming.*", "ingestion_date", "end_validity_date")
    print("start new only records")
    # Trovare i nuovi record che non esistono nel DataFrame corrente
    new_only_records = incoming_df.alias("incoming").join(
        self.alias("current"),
        join_condition,
        "left_anti"
    ).withColumn("ingestion_date", current_timestamp) \
     .withColumn("end_validity_date", F.lit(None).cast(TimestampType()))
    print("start unchanged records")
    # Gestione dei record non modificati
    unchanged_records = self.alias("current").join(
        changes_detected.alias("changes"),
        on=key_list,
        how="left_anti"
    )
    print("union by name")
    # Combinare i record non modificati, inattivi, nuovi e quelli solo nuovi
    final_df = unchanged_records.unionByName(inactive_records).unionByName(new_records).unionByName(new_only_records)
    print("FINALLY RETURN")
    return final_df
setattr(DataFrame, "compute_scd2", compute_scd2)



####### START PROCESSING

# check data from bucket
response = s3_client.list_objects_v2(Bucket=f"{SourceBucketName}")

if "Contents" not in response:
    job.commit() # Only necessary if you are loading data from s3 and you have job bookmarks enabled.
    os._exit(0)


##### READ NEW DATA FROM S3
if mode == 'Full':
    # get last inserted files
    list_files = [content for content in response['Contents'] ]
    last_insert = max(list_files, key=lambda x: x['LastModified'])
    sub_path = "/".join(last_insert['Key'].split("/")[:-2])
    source_files = [f"s3://{SourceBucketName}/{sub_path}/*"]

## mode == 'Delta'
else:
    # read last insertion date
    last_insertion_date = get_last_insertion_date(f"{target_schema_name}.{target_table_name}")
    sub_paths = ["/".join(content['Key'].split("/")[:-2]) for content in response['Contents'] if content['LastModified'].replace(tzinfo=timezone.utc) > last_insertion_date ]
    source_files = [f"s3://{SourceBucketName}/{sub_path}/*" for sub_path in sub_paths]


if len(source_files) == 0:
    job.commit()
    os._exit(0)

# read last data
new_data = spark.read \
    .option("datetimeRebaseMode", "LEGACY") \
    .option("recursiveFileLookup", True) \
    .parquet(*source_files)


# LEGACY

if target_table_name in  ("sap_sls_ft_i_salesdocument_vbak"):
    print("FIXING DATE 0224-09-09 00:00:00")
    new_data = new_data.withColumn("CustomerPurchaseOrderDate",
        when(col("CustomerPurchaseOrderDate") == "0224-09-09 00:00:00", lit(None))
        .otherwise(col("CustomerPurchaseOrderDate"))
    )

if target_table_name in  ( "sap_sls_ft_zww_otc_vbkd"):
    print("FIXING DATE 0224-09-09 00:00:00")
    new_data = new_data.withColumn("bstdk",
        when(col("bstdk") == "0224-09-09 00:00:00", lit(None))
        .otherwise(col("bstdk"))
    )


for c in p_key:
    new_data = new_data.withColumn(c, coalesce(col(c), lit("")))



# compute SCD Type 2 based on list pkeys
# save result on redshift
if mode == 'Full':
    print("Save to redshift")
    new_data.save_redshift(f"{target_schema_name}.{target_table_name}")
    print("Done")
else:
    # read current data from table
    print("Read redshift")
    data_table = read_redshift(f"{target_schema_name}.{target_table_name}")
    print("Read redshift Done")
    print("Compute SCD2")
    updated_data = data_table.compute_scd2(new_data, *p_key)
    print("Save to redshift")
    updated_data.save_redshift(f"{target_schema_name}.{target_table_name}")
    print("Save to redshift Done")
