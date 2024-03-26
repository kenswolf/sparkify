import json
import boto3


def list_s3_files(s3_resourse, prefix: str, limit: int = None):
    """  sample use:  key = list_s3_files(s3_client, "song-data/A/A/A") """

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
