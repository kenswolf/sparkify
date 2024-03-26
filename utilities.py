""" Utility Functions """
import json
import time
import boto3


def list_s3_files(s3_resourse, prefix: str, limit: int = None):
    """ sample use:  key = list_s3_files(s3_client, "song-data/A/A/A") """

    bucket = s3_resourse.Bucket("udacity-dend")

    objs = bucket.objects.filter(Prefix=prefix)

    i = 0
    for obj in objs:
        key = obj.key
        print(key)
        i += 1
        if limit is not None and i == limit:
            break

    return key


def count_files(s3_client, prefix: str):
    """ sample use:  count_file(s3_client, 'song-data') """

    count = 0
    paginator = s3_client.get_paginator('list_objects_v2')
    # 400,150 song files!
    for page in paginator.paginate(Bucket="udacity-dend", Prefix=prefix):
        count += len(page.get('Contents', []))
    print(f"Number of files with prefix '{prefix}': {count}")


def download_file(s3_client, key):
    """ sample use:  download_file(s3_client, 'song-data/A/Y/F/TRAYFUW128F428F618.json' ) """

    modified_key = key.replace("/", "-")
    s3_client.download_file("udacity-dend", key, modified_key)
    print("File downloaded successfully")
    with open(modified_key, 'rt') as f:
        data = json.load(f)
        print(data)

def get_duration_string(start_time):
    """ Create printable string of how long a process has taken so far"""

    duration = time.time() - start_time

    duration_minutes = int(duration // 60)
    duration_seconds = int(duration % 60)

    if duration_minutes == 0:
        duration_string = f"{duration_seconds} seconds"
    elif duration_minutes == 1:
        duration_string = f"1 minute and {duration_seconds} seconds"
    else:
        duration_string = f"{duration_minutes} minutes {duration_seconds} seconds"

    return duration_string

def get_host(DWH_WORKGROUP_NAME:str, DWH_REGION:str, KEY:str, SECRET:str) -> str:
    """ Gets the string of the host / endpoint that is used for ingress requests """

    redshift_serverless_client = boto3.client('redshift-serverless',
                                              region_name=DWH_REGION,
                                              aws_access_key_id=KEY,
                                              aws_secret_access_key=SECRET,
                                              )

    workgroup = redshift_serverless_client.get_workgroup(
        workgroupName=DWH_WORKGROUP_NAME)
    endpoint = workgroup['workgroup']['endpoint']
    return endpoint['address']
