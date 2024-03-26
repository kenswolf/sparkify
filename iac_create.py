""" This creates an AWS Serverless Redshift data warehouse

BEFORE RUNNING THIS PROGRAM 
    1)  Log into AWS Console
    2)  create iam user  with the "AdministratorAccess" policy/privilages 
    3)  generate secret and key for the new user
    4)  put the secret and key into dwh.cfg config file 

WHAT THIS PROGRAM DOES (USING THE BOTO3 LIBRARY)

    1)  Run create_serverless_redshift (ie python3 create_serverless_redshift.py)
    
        creates a role that allows redshift to make calls to AWS services 
    
    2)  gives the new role the policy/privilages to read s3

    3)  creates Redshift Serverless 'Namespace' 
        (this auto-magically creates a database, using configuration in the dwh.cfg config file) 
        (new role's ARN is passed in, so the new Redshift Serverless 'Namespace' has the new role)

    4)  create Redshift Serverless 'Workgroup' 
        which is a group of compute resources.  
        (note a namespace can have multiple workgroups 
        and they can be used selectively as needed,
        but we only need one for this program) """

import configparser
import json
import time
import boto3
from botocore.exceptions import ClientError
import ingress_rule
import utilities

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")

DWH_REGION = config.get("DWH", "DWH_REGION")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_NAMESPACE_NAME = config.get("DWH", "DWH_NAMESPACE_NAME")
DWH_WORKGROUP_NAME = config.get("DWH", "DWH_WORKGROUP_NAME")


def setup_namespace(redshift_serverless: boto3.session.Session.resource,
                    role_arn: str) -> None:
    """ Create Namespace (if it does not exist) for/as-part-of the new Redshift """
    namespace_exists = False
    response = redshift_serverless.list_namespaces()
    for namespace in response['namespaces']:
        if DWH_NAMESPACE_NAME == namespace['namespaceName']:
            namespace_exists = True
            break

    if namespace_exists is True:
        print(f"A namespace {DWH_NAMESPACE_NAME} already exists")
    else:
        # https://boto3.amazonaws.com/v1/documentation/api/1.26.2/reference/services/redshift-serverless.html#RedshiftServerless.Client.create_namespace
        response = redshift_serverless.create_namespace(
            adminUserPassword=DWH_DB_PASSWORD,
            adminUsername=DWH_DB_USER,
            dbName=DWH_DB,
            namespaceName=DWH_NAMESPACE_NAME,
            # Optionally specify other parameters like KMS key, admin username/password, etc.
            # 'arn:aws:iam::123456789012:role/YourRedshiftRole']
            iamRoles=[role_arn]
        )

        # print(response)
        print('Initiated creation of namespace', DWH_NAMESPACE_NAME,
              'with default database settings from config file.')

    # response = redshift_serverless.list_namespaces()
    # for namespace in response['namespaces']:
    #    print(f"Namespace: {namespace['namespaceName']}")


def setup_workgroup(redshift_serverless: boto3.session.Session.resource) -> None:
    """ Create workgroup (if it does not exist) for/as-part-of the new Redshift """

    workgroup_exists = False
    response = redshift_serverless.list_workgroups()
    for wg in response['workgroups']:
        if wg['workgroupName'] == DWH_WORKGROUP_NAME:
            workgroup_exists = True

    if workgroup_exists is True:
        print(
            f"A workgroup {DWH_WORKGROUP_NAME} for namespace {DWH_NAMESPACE_NAME} already exists")
    else:
        # Create a workgroup
        response = redshift_serverless.create_workgroup(
            workgroupName=DWH_WORKGROUP_NAME,
            baseCapacity=8,  # Specify base capacity units, cannot be less than 8 RPUs
            namespaceName=DWH_NAMESPACE_NAME,  # Specify the namespace
            # Specify VPC routing preferences, ie just route over internet
            enhancedVpcRouting=False,
            publiclyAccessible=True,  # Specify access preferences
            # Other optional parameters...
        )

        # print(response)
        print('Initiated creation of workgroup', DWH_WORKGROUP_NAME,
              'for namespace', DWH_NAMESPACE_NAME)


def get_role_arn(iam_client, role_name):
    """ retrieve and return the ARN of the role we have given permissions to """
    role_arn = iam_client.get_role(RoleName=role_name)['Role']['Arn']
    return role_arn


def setup_role_for_redshit_to_access_s3():
    """ get_role_that_redshit_will_use_to_access_s3_data(iam_client, DWH_IAM_ROLE_NAME) """

    iam_client = boto3.client('iam',
                              region_name=DWH_REGION,
                              aws_access_key_id=KEY,
                              aws_secret_access_key=SECRET
                              )

    try:
        print("Creating an IAM Role", DWH_IAM_ROLE_NAME,
              "that allows Redshift to make AWS calls.")

        iam_client.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )

        print("Adding an S3 Read Policy to the IAM Role", DWH_IAM_ROLE_NAME)

        status = iam_client.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']

    except ClientError as error:
        if error.response['Error']['Code'] == 'EntityAlreadyExists':
            print(f"The role {DWH_IAM_ROLE_NAME} already exists.")
        else:
            raise

    return get_role_arn(iam_client, DWH_IAM_ROLE_NAME)


def wait_until_workgroup_ready(redshift_serverless_client):
    """ Monitors the status of the new workgroup, 
    in a loop, until it is ready for use"""

    while True:
        start_time = time.time()

        response = redshift_serverless_client.get_workgroup(
            workgroupName=DWH_WORKGROUP_NAME)
        status = response['workgroup']['status']

        if status == 'AVAILABLE':

            duration_string = utilities.get_duration_string(start_time)
            print(
                f"Workgroup {DWH_WORKGROUP_NAME} created. " +
                f"It took {duration_string}. It is now ready for use.")
            break
        elif status == 'FAILED':
            print(f"Workgroup {DWH_WORKGROUP_NAME} creation failed.")
            break
        else:
            duration_string = utilities.get_duration_string(start_time)
            print(
                f"Workgroup {DWH_WORKGROUP_NAME} creation in-progress. " +
                f"It has been running for {duration_string}.")
            time.sleep(10)  # Wait for 10 seconds before checking again


def wait_until_namespace_ready(redshift_serverless_client):
    """ Monitors the status of the new namespace, 
    in a loop, until it is ready for use"""

    start_time = time.time()

    while True:

        response = redshift_serverless_client.get_namespace(
            namespaceName=DWH_NAMESPACE_NAME)
        status = response['namespace']['status']

        if status == 'AVAILABLE':

            duration_string = utilities.get_duration_string(start_time)
            print(
                f"Namespace {DWH_NAMESPACE_NAME} created. " +
                f"It took {duration_string}. It is now ready for use.")
            break
        elif status == 'FAILED':
            print(f"Namespace {DWH_NAMESPACE_NAME} creation failed.")
            break
        else:
            duration_string = utilities.get_duration_string(start_time)
            print(
                f"Namespace {DWH_NAMESPACE_NAME} creation in-progress. " +
                f"It has been running for {duration_string}.")
            time.sleep(10)  # Wait for 10 seconds before checking again


def main():
    """ This method controls the IAC creation processes """

    role_arn = setup_role_for_redshit_to_access_s3()

    redshift_serverless_client = boto3.client('redshift-serverless',
                                              region_name=DWH_REGION,
                                              aws_access_key_id=KEY,
                                              aws_secret_access_key=SECRET
                                              )

    setup_namespace(redshift_serverless_client, role_arn)

    setup_workgroup(redshift_serverless_client)

    ingress_rule.setup_ingress_rule()

    wait_until_namespace_ready(redshift_serverless_client)

    wait_until_workgroup_ready(redshift_serverless_client)


if __name__ == "__main__":
    main()
