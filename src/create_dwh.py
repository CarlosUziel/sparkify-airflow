import json
import logging
from configparser import ConfigParser
from pathlib import Path
from time import sleep
from typing import Any, Dict

import boto3

from utils import open_db_port, process_config


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
        logging.warn(e)

    # 2. Attach role
    iam_client.attach_role_policy(
        RoleName=dwh_config.get("DWH", "DWH_IAM_ROLE_NAME"),
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

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
    print("Waiting for Redshift cluster to become available...")
    while True:
        cluster_props = redshift_client.describe_clusters(
            ClusterIdentifier=dwh_config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
        )["Clusters"][0]

        if cluster_props["ClusterStatus"] == "available":
            break
        else:
            sleep(30)
    print("Redshift cluster is ready to be used!")

    return cluster_props, redshift_client


def main():
    # 0. Process configuration files
    user_config, dwh_config = (
        process_config(Path(__file__).parents[1].joinpath("_user.cfg")),
        process_config(Path(__file__).parents[1].joinpath("dwh.cfg")),
    )

    # 1. Get AWS clients
    iam_client, redshift_client = [
        boto3.client(
            client,
            aws_access_key_id=user_config.get("AWS", "KEY"),
            aws_secret_access_key=user_config.get("AWS", "SECRET"),
            region_name=dwh_config.get("GENERAL", "REGION"),
        )
        for client in ("iam", "redshift")
    ]

    # 2. Create necessary role
    iamArn = create_attach_role(iam_client, dwh_config)

    # 3. Create Redshift cluster
    cluster_props, redshift_client = create_redshift_cluster(
        redshift_client, dwh_config, iamArn
    )

    # 4. Add some cluster props to configuration file
    dwh_config.set("DWH", "DWH_ENDPOINT", cluster_props["Endpoint"]["Address"])
    dwh_config.set("DWH", "DWH_ROLE_ARN", cluster_props["IamRoles"][0]["IamRoleArn"])
    dwh_config.set("DWH", "DWH_VPC_ID", cluster_props["VpcId"])

    with Path(__file__).parents[1].joinpath("dwh.cfg").open("w") as fp:
        dwh_config.write(fp)

    # 5. Open TCP port
    open_db_port(user_config, dwh_config)

    return cluster_props, redshift_client, iam_client


if __name__ == "__main":
    main()
