from configparser import ConfigParser
from pathlib import Path
from typing import Any

import boto3
import psycopg2


def process_config(config_path: Path) -> ConfigParser:
    """Process a single configuration file."""
    assert config_path.exists(), (
        f"User configuration file {config_path} does not exist, "
        "please create it following the README.md file of this project."
    )

    with config_path.open("r") as fp:
        config = ConfigParser()
        config.read_file(fp)

    return config


def open_db_port(user_config: ConfigParser, dwh_config: ConfigParser):
    """Open an incoming  TCP port to access the cluster endpoint"""

    # 1. Get EC2 client
    ec2 = boto3.resource(
        "ec2",
        aws_access_key_id=user_config.get("AWS", "KEY"),
        aws_secret_access_key=user_config.get("AWS", "SECRET"),
        region_name=dwh_config.get("GENERAL", "REGION"),
    )

    # 2. Open DB port
    try:
        vpc = ec2.Vpc(id=dwh_config.get("DWH", "DWH_VPC_ID"))
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(dwh_config.get("DWH", "DWH_DB_PORT")),
            ToPort=int(dwh_config.get("DWH", "DWH_DB_PORT")),
        )
    except Exception as e:
        print(e)


def delete_cluster(redshift_client: Any, dwh_config: ConfigParser):
    """Delete an Amazon Redshift cluster

    Args:
        redshift_client: A boto3 Redshift client
        dwh_config: DB configuration parameters.
    """
    redshift_client.delete_cluster(
        ClusterIdentifier=dwh_config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
        SkipFinalClusterSnapshot=True,
    )


def delete_iam_roles(iam_client: Any, dwh_config: ConfigParser):
    """Delete IAM roles

    Args:
        iam_client: A boto3 IAM client
        dwh_config: DB configuration parameters.
    """
    iam_client.detach_role_policy(
        RoleName=dwh_config.get("DWH", "DWH_IAM_ROLE_NAME"),
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    iam_client.delete_role(RoleName=dwh_config.get("DWH", "DWH_IAM_ROLE_NAME"))


def get_db_connection(dwh_config: ConfigParser):
    """Get database connection and cursor objects"""
    conn = psycopg2.connect(
        f"host={dwh_config.get('DWH', 'DWH_ENDPOINT')} "
        f"dbname={dwh_config.get('DWH', 'DWH_DB')} "
        f"user={dwh_config.get('DWH', 'DWH_DB_USER')} "
        f"password={dwh_config.get('DWH', 'DWH_DB_PASSWORD')} "
        f"port={dwh_config.get('DWH', 'DWH_DB_PORT')} "
    )
    cur = conn.cursor()
    return conn, cur
