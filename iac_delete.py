""" Used to delete the AWS Serverless Redshift """
import configparser
import time
import boto3
from botocore.exceptions import ClientError
import utilities

config = configparser.ConfigParser()
config.read_file('dwh_iac.cfg')
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
DWH_REGION = config.get("DWH", "DWH_REGION")
DWH_NAMESPACE_NAME = config.get("DWH", "DWH_NAMESPACE_NAME")
DWH_WORKGROUP_NAME = config.get("DWH", "DWH_WORKGROUP_NAME")


def delete_workgroup(redshift_serverless_client):
    """ Deletes the workgroup of the  AWS Serverless Redshift """

    start_time = None
    while True:

        try:
            response = redshift_serverless_client.delete_workgroup(
                workgroupName=DWH_WORKGROUP_NAME)
            print(
                f"Workgroup {DWH_WORKGROUP_NAME} deletion initiated successfully.")
            start_time = time.time()

        except ClientError as error:
            code = error.response['Error']['Code']

            if code == 'ConflictException' and start_time is not None:
                duration_string = utilities.get_duration_string(start_time)
                print(
                    f"Workgroup {DWH_WORKGROUP_NAME} deletion in-progress. " +
                    f"It has been running for {duration_string}.")
            elif code == 'ResourceNotFoundException' and start_time is not None:
                duration_string = utilities.get_duration_string(start_time)
                print(
                    f"Workgroup {DWH_WORKGROUP_NAME} deletion completed successfully. " +
                    f"It took {duration_string}. Now the namespace deletion can be initiated.")
                break
            else:
                error_msg_prefix = f"Error deleting workgroup {DWH_WORKGROUP_NAME}: {code} :"

                if code == 'ConflictException':
                    print(error_msg_prefix,
                          "There is an operation running on the workgroup.")
                elif code == 'ValidationException':
                    print(error_msg_prefix, "The input provided was invalid.")
                elif code == 'InternalServerException':
                    print(error_msg_prefix, "An internal server error occurred.")
                elif code == 'ResourceNotFoundException':
                    print(error_msg_prefix, "Resource does not exist")
                else:
                    print(error_msg_prefix, f"{str(error)}")
                break

        time.sleep(10)


def delete_namespace(redshift_serverless_client):
    """ Deletes the namespace of the  AWS Serverless Redshift """

    start_time = None
    while True:

        try:
            response = redshift_serverless_client.delete_namespace(
                namespaceName=DWH_NAMESPACE_NAME)
            print(
                f"Namespace {DWH_NAMESPACE_NAME} deletion initiated successfully.")
            start_time = time.time()

        except ClientError as error:
            code = error.response['Error']['Code']

            if code == 'ConflictException' and start_time is not None:
                duration_string = utilities.get_duration_string(start_time)
                print(
                    f"Namespace {DWH_NAMESPACE_NAME} deletion in-progress. " +
                    f"It has been running for {duration_string}.")
            elif code == 'ResourceNotFoundException' and start_time is not None:
                duration_string = utilities.get_duration_string(start_time)
                print(
                    f"Namespace {DWH_NAMESPACE_NAME} deletion completed successfully. " +
                    f"It took {duration_string}.")
                break
            else:

                error_msg_prefix = f"Error deleting namespace {DWH_NAMESPACE_NAME}: {code} :"

                if code == 'ConflictException':
                    print(error_msg_prefix,
                          "There is an operation running on the namespace.")
                elif code == 'ValidationException':
                    print(error_msg_prefix, "The input provided was invalid.")
                elif code == 'InternalServerException':
                    print(error_msg_prefix, "An internal server error occurred.")
                elif code == 'ResourceNotFoundException':
                    print(error_msg_prefix, "Resource does not exist")
                else:
                    print(error_msg_prefix, f"{str(error)}")
                break

        time.sleep(10)


def main():
    """ This method controls the IAC deletion/cleanup processes """

    redshift_serverless_client = boto3.client('redshift-serverless',
                                              region_name=DWH_REGION,
                                              aws_access_key_id=KEY,
                                              aws_secret_access_key=SECRET
                                              )

    delete_workgroup(redshift_serverless_client)
    delete_namespace(redshift_serverless_client)


if __name__ == "__main__":
    main()
