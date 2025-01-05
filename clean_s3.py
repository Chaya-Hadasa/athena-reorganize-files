import boto3
import pandas as pd
import time
import io
bucket_name = "your_bucket_name"
table_schema = "your_schema"
s3 = boto3.client("s3")
DATABASE = "information_schema"
OUTPUT_BUCKET = f"s3://{bucket_name}/tmp/"
def get_athena_tables(bucket_name, prefix=""):

    QUERY = f"""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = {table_schema}
    and table_type = 'BASE TABLE'
    """

    # Initialize Athena client
    athena_client = boto3.client('athena')
    s3 = boto3.client('s3')

    # Start query execution
    response = athena_client.start_query_execution(
        QueryString=QUERY,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT_BUCKET}
    )

    query_execution_id = response['QueryExecutionId']
    print(f"Query Execution ID: {query_execution_id}")

    # Wait for the query to finish
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        print("Query is still running...")
        time.sleep(5)

    if status == 'SUCCEEDED':
        print("Query completed successfully.")

        # Fetch the results from S3 directly into memory
        output_file_path = f"{OUTPUT_BUCKET}{query_execution_id}.csv"
        print(f"Results available at: {output_file_path}")

        # Extract bucket name and file key
        bucket_name = OUTPUT_BUCKET.replace("s3://", "").split('/')[0]
        key_prefix = "/".join(OUTPUT_BUCKET.replace("s3://", "").split('/')[1:])
        key = f"{key_prefix}{query_execution_id}.csv"

        # Get the S3 object
        s3_object = s3.get_object(Bucket=bucket_name, Key=key)

        # Load the object content into a pandas DataFrame
        file_content = s3_object['Body'].read()
        df = pd.read_csv(io.BytesIO(file_content))

        # Display the DataFrame
        print(df)
        return df
    else:
        print(f"Query failed with status: {status}")

def get_s3_objects_use_for_athena_tables(bucket_name):
    final_query=""
    df_tables = get_athena_tables(bucket_name)
    for index, row in df_tables.iterrows():
        table_schema = row["table_schema"]
        table_name = row["table_name"]

        # Create the query for the current table
        concat_query = f"""SELECT DISTINCT "$path" AS path, '{table_name}' as table_name FROM {table_schema}.{table_name}"""

        if final_query:
            final_query += " UNION ALL\n" + concat_query
        else:
            final_query = concat_query

    base_query = f"""select replace(path,'s3://{bucket_name}/','') as path, table_name as table_name  from({final_query})"""
    athena_client = boto3.client('athena')
    s3 = boto3.client('s3')

    # Start query execution
    response = athena_client.start_query_execution(
        QueryString=base_query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT_BUCKET}
    )

    query_execution_id = response['QueryExecutionId']
    print(f"Query Execution ID: {query_execution_id}")

    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        print("Query is still running...")
        time.sleep(5)

    if status == 'SUCCEEDED':
        print("Query completed successfully.")

        # Fetch the results from S3 directly into memory
        output_file_path = f"{OUTPUT_BUCKET}{query_execution_id}.csv"
        print(f"Results available at: {output_file_path}")

        # Extract bucket name and file key
        bucket_name = OUTPUT_BUCKET.replace("s3://", "").split('/')[0]
        key_prefix = "/".join(OUTPUT_BUCKET.replace("s3://", "").split('/')[1:])
        key = f"{key_prefix}{query_execution_id}.csv"

        # Get the S3 object
        s3_object = s3.get_object(Bucket=bucket_name, Key=key)

        # Load the object content into a pandas DataFrame
        file_content = s3_object['Body'].read()
        df = pd.read_csv(io.BytesIO(file_content))

        return df
    else:
        print(f"Query failed with status: {status}")
        return []

def list_s3_objects(bucket_name, prefix=""):
    paginator = s3.get_paginator("list_objects_v2")
    result = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            result.append({
                "Date": obj["LastModified"].strftime("%Y-%m-%d"),
                "Time": obj["LastModified"].strftime("%H:%M:%S"),
                "Size": obj["Size"],
                "Path": obj["Key"]
            })
    return result



based_athena_files_df = get_s3_objects_use_for_athena_tables(bucket_name)
based_athena_files = based_athena_files_df['path'].tolist()
objects = list_s3_objects(bucket_name)

all_obj_on_bucket_df = pd.DataFrame(objects)
all_obj_on_bucket_files = all_obj_on_bucket_df['Path'].tolist()
result_df = based_athena_files_df.merge(all_obj_on_bucket_df, left_on='path', right_on='Path', how='inner')
result_df.to_csv('result.csv', index=False)

to_remove = [item for item in all_obj_on_bucket_files if item not in based_athena_files and '/' not in item and item != '69c98aa2-1876-45d6-bf3a-ab3d72dcf032.csv']

print('all_obj_on_bucket_files count=',len(all_obj_on_bucket_files) )
print('based_athena_files count=',len(based_athena_files) )
# df_a[~df_a['values'].isin(df_b['values'])]
print('to_remove count=',len(to_remove) )

df = pd.DataFrame({'values': to_remove})

df.to_csv('to_remove.csv', index=False)

for file_name in to_remove:
    print(file_name)
    s3.copy_object(Bucket=bucket_name, CopySource=f"{bucket_name}/{file_name}", Key=f"to_delete/{file_name}")
    s3.delete_object(Bucket=bucket_name, Key=file_name)





