import configparser
import requests
import boto3

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
DWH_REGION = config.get("DWH", "DWH_REGION")
DWH_WORKGROUP_NAME = config.get("DWH", "DWH_WORKGROUP_NAME")


def _get_cidr_ip():
    response = requests.get('https://httpbin.org/ip')
    ip_address = response.json()['origin']
    cidr_ip = ip_address + '/32'
    return cidr_ip


def _get_security_group_id(redshift_serverless_client):
    response = redshift_serverless_client.list_workgroups()
    for wg in response['workgroups']:
        if wg['workgroupName'] == DWH_WORKGROUP_NAME:
            security_group_id = wg['securityGroupIds'][0]
            break
    return security_group_id


def _ingress_rule_exists(ec2_client, security_group_id):
    response = ec2_client.describe_security_groups(
        GroupIds=[security_group_id])
    rule_exists = False
    for security_group in response.get('SecurityGroups', []):
        rules = security_group.get('IpPermissions', [])
        for rule in rules:
            if ((rule['ToPort'] == rule['FromPort'] == 5439) and rule['IpProtocol'] == 'tcp' and rule['IpRanges'][0]['CidrIp'] == '23.93.180.103/32'):
                rule_exists = True
                print('Rule exists', rule['IpRanges'][0]['CidrIp'], rule)
                break

    return rule_exists


def _create_ingress_rule(ec2_client, security_group_id, cidr_ip):

    ingress_params = {
        'GroupId': security_group_id,
        'IpPermissions': [{
            'IpProtocol': 'tcp',
            'FromPort': 5439,
            'ToPort': 5439,
            'IpRanges': [{'CidrIp': cidr_ip}]
        }]
    }
    response = ec2_client.authorize_security_group_ingress(
        **ingress_params)
    print('Creating ingress rule... Responce to rule creation', response)


def setup_ingress_rule():

    rs_client = boto3.client('redshift-serverless',
                             region_name=DWH_REGION,
                             aws_access_key_id=KEY,
                             aws_secret_access_key=SECRET,
                             )

    ec2_client = boto3.client('ec2',
                              region_name=DWH_REGION,
                              aws_access_key_id=KEY,
                              aws_secret_access_key=SECRET
                              )

    cidr_ip = _get_cidr_ip()
    security_group_id = _get_security_group_id(rs_client)
    rule_exists = _ingress_rule_exists(ec2_client, security_group_id)
    if rule_exists is False:
        _create_ingress_rule(ec2_client, security_group_id, cidr_ip)
