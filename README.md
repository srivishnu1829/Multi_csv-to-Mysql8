# Multi_csv-to-Mysql8
config included

------------------------------------------------multi csv to mysql8
import sys
import yaml
import boto3
import json
import datetime

from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions

body = """
Json to DWH Load Job status
Start Time: {job_start_time}

-------------------------------------------
{message}
-------------------------------------------

End Time: {job_end_time}
Total time taken: {total_time}"""

file_status_message = """
Start Time: {start_time}
File Name: {file_name}
File Size: {file_size}
End Time: {end_time}
Total time taken: {total_time}
Load Status: {status}

"""

def trigger_sn(message):

    client = boto3.client('sns', region_name='ap-south-1')
    response = client.publish(
    TargetArn="arn:aws:sns:ap-south-1:219388852116:glue_job_alerts",
    Message=json.dumps({'default': json.dumps(message)}),
    Subject='Job Failed - Json to DWH data load',
    MessageStructure='json')


def read_yaml_from_s3(s3_client, bucket_name, yaml_file_prefix):

    file_content = s3_client.get_object(Bucket=bucket_name, Key=yaml_file_prefix)["Body"].read()
    yaml_content = yaml.safe_load(file_content.decode("utf-8"))
    return yaml_content

def create_relationalize_df(glueContext, json_path, table_name):

    dyf = glueContext.create_dynamic_frame.from_options('s3', connection_options={"paths": [json_path]}, format='csv', transformation_ctx=table_name)
    relationalize_dyf = Relationalize.apply(frame = dyf, staging_path ='s3://dbmigrate1/temp_glue_connections', name = table_name, transformation_ctx = table_name)
    return relationalize_dyf.select(table_name).toDF()

def write_dataframe_into_jdbc(df, url, table_name, user, password):

    table_name=table_name
    db_properties={"user": user, "password": password}
    df.write.option("truncate", "true").jdbc(url= url , table= table_name , mode="overwrite", properties=db_properties)

def execute(s3_client, bucket_name, config_file_prefix, glueContext):

    config_list = read_yaml_from_s3(s3_client, bucket_name, config_file_prefix)
    job_status = 'SUCCESS'
    message = ''
    error_message = {}

    for config in config_list:

        start_time = datetime.datetime.now()
        source_file_prefix = config['source_file_prefix']
        target_table_name = config['target_table_name']
        target_db_url = config['target_db_url']
        target_db_user_name = config['target_db_user_name']
        target_db_password = config['target_db_password']

        try:
            source_file_path = f"s3://{bucket_name}/{source_file_prefix}"
            file_size = s3_client.head_object(Bucket=bucket_name, Key=source_file_prefix)['ContentLength']
            df = create_relationalize_df(glueContext, source_file_path, target_table_name)
            write_dataframe_into_jdbc(df, target_db_url, target_table_name, target_db_user_name, target_db_password)

            end_time = datetime.datetime.now()
            total_time = (end_time - start_time)

            message = message + file_status_message.format(start_time=str(start_time), file_name=source_file_path, file_size=file_size, end_time=str(end_time), total_time=str(total_time), status='SUCCESS')
            # s3_client.delete_object(Bucket=bucket_name, Key=source_file_prefix)


        except Exception as e:
            job_status = 'FAIL'
            error_message[target_table_name] = f'Data not loaded into {target_table_name}'
            message = message + file_status_message.format(start_time=str(start_time), file_name=source_file_path, file_size=file_size, end_time='NA', total_time='NA', status='FAILED') + str(e)

    return job_status, message, error_message


def main():

    start_time = datetime.datetime.now()

    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "config_file_prefix", 'bucket_name']
    )

    config_file_prefix = args["config_file_prefix"]
    bucket_name = args["bucket_name"]

    s3_client = boto3.client("s3")
    sc = SparkContext()
    glueContext = GlueContext(sc)

    job_status, message, error_message = execute(
        s3_client,
        bucket_name,
        config_file_prefix,
        glueContext
    )

    end_time = datetime.datetime.now()
    total_time = (end_time - start_time)

    final_message = body.format(job_start_time=str(start_time), job_end_time=str(end_time), total_time=str(total_time), message=message)
    print(final_message)


    if job_status == "FAIL":
        trigger_sn(error_message)
        raise Exception("Job Failed ")


if __name__ == "__main__":
    main()


