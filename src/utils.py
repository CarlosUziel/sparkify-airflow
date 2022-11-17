import json
import logging
from configparser import ConfigParser
from pathlib import Path
from time import sleep
from typing import Any, Dict

import boto3
import psycopg2
from airflow import settings
from airflow.models import Connection

from sql_queries import (
    STAGING_TABLES,
    STAR_TABLES,
    STAR_TABLES_CONSTRAINTS,
    STAR_TABLES_DISTSTYLES,
    get_create_table_query,
    get_drop_table_query,
)


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


def create_attach_role(iam_client: Any, dwh_config: ConfigParser) -> str:
    """Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)

    Args:
        iam_client: client to access AWS IAM service.
        dwh_config: a ConfigParser containing necessary parameters.
    """
    # 1. Create the role
    try:
        iam_client.create_role(
            Path="/",
            RoleName=dwh_config.get("DWH", "DWH_IAM_ROLE_NAME"),
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        logging.error(e)

    # 2. Attach role
    try:
        iam_client.attach_role_policy(
            RoleName=dwh_config.get("DWH", "DWH_IAM_ROLE_NAME"),
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )["ResponseMetadata"]["HTTPStatusCode"]
    except Exception as e:
        logging.error(e)

    # 3. Return IAM role ARN
    return iam_client.get_role(RoleName=dwh_config.get("DWH", "DWH_IAM_ROLE_NAME"))[
        "Role"
    ]["Arn"]


def create_redshift_cluster(
    redshift_client: Any, dwh_config: ConfigParser, iamArn: str
) -> Dict:
    """Create a Redshift cluster with given parameters and appropiate role.

    Args:
        iam_client: client to access AWS Redshift service.
        dwh_config: a ConfigParser containing necessary parameters.
        iamArn: IAM role.
    """
    # 1. Create cluster
    try:
        redshift_client.create_cluster(
            # HW
            ClusterType=dwh_config.get("DWH", "DWH_CLUSTER_TYPE"),
            NodeType=dwh_config.get("DWH", "DWH_NODE_TYPE"),
            NumberOfNodes=int(dwh_config.get("DWH", "DWH_NUM_NODES")),
            # Identifiers & Credentials
            DBName=dwh_config.get("DWH", "DWH_DB"),
            ClusterIdentifier=dwh_config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
            MasterUsername=dwh_config.get("DWH", "DWH_DB_USER"),
            MasterUserPassword=dwh_config.get("DWH", "DWH_DB_PASSWORD"),
            # Roles (for s3 access)
            IamRoles=[iamArn],
        )

    except Exception as e:
        logging.error(e)

    # 2. Wait for cluster to be available
    logging.info("Waiting for Redshift cluster to become available...")
    while True:
        cluster_props = redshift_client.describe_clusters(
            ClusterIdentifier=dwh_config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
        )["Clusters"][0]

        if cluster_props["ClusterStatus"] == "available":
            break
        else:
            sleep(30)
    logging.info("Redshift cluster is ready to be used!")

    return cluster_props, redshift_client


def open_db_port(user_config: ConfigParser, dwh_config: ConfigParser):
    """Open an incoming  TCP port to access the cluster endpoint"""

    # 1. Get EC2 client
    ec2 = boto3.resource(
        "ec2",
        aws_access_key_id=user_config.get("AWS", "AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=user_config.get("AWS", "AWS_SECRET_ACCESS_KEY"),
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


def drop_tables(cur: Any, conn: Any):
    """Drop staging, fact and dimension tables"""
    for table_name in [*STAR_TABLES.keys(), *STAGING_TABLES.keys()]:
        cur.execute(get_drop_table_query(table_name))
        conn.commit()


def create_tables(cur: Any, conn: Any):
    """Create staging, fact and dimension tables"""
    for table_name, table_args in [*STAR_TABLES.items(), *STAGING_TABLES.items()]:
        cur.execute(
            get_create_table_query(
                table_name, [*table_args, *STAR_TABLES_CONSTRAINTS.get(table_name, [])]
            )
            + f" {STAR_TABLES_DISTSTYLES.get(table_name, '')}"
        )
        conn.commit()


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


def register_connection(**kwargs):
    """Register Airflow connection"""
    conn = Connection(**kwargs)
    session = settings.Session()
    try:
        session.add(conn)
        session.commit()
    except Exception as e:
        logging.error(e)
        session.rollback()
