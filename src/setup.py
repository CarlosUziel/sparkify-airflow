import logging
import os
import warnings
from pathlib import Path

os.environ["AIRFLOW_HOME"] = str(
    Path(__file__).parents[1].joinpath("_airflow").resolve()
)

import boto3
import psycopg2
from rich import traceback

from utils import (
    create_attach_role,
    create_redshift_cluster,
    create_tables,
    drop_tables,
    get_db_connection,
    open_db_port,
    process_config,
    register_connection,
)

_ = traceback.install()
logging.basicConfig(force=True)
logging.getLogger().setLevel(logging.INFO)
warnings.filterwarnings("ignore")


def main():
    # 0. Process configuration files
    user_config, dwh_config = (
        process_config(Path(__file__).parents[1].joinpath("_user.cfg")),
        process_config(Path(__file__).parents[1].joinpath("dwh.cfg")),
    )

    # 1. AWS
    # 1.1. Get AWS clients
    iam_client, redshift_client = [
        boto3.client(
            client,
            aws_access_key_id=user_config.get("AWS", "AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=user_config.get("AWS", "AWS_SECRET_ACCESS_KEY"),
            region_name=dwh_config.get("GENERAL", "REGION"),
        )
        for client in ("iam", "redshift")
    ]

    # 1.2. Create necessary role
    iamArn = create_attach_role(iam_client, dwh_config)

    # 1.3. Create Redshift cluster
    cluster_props, redshift_client = create_redshift_cluster(
        redshift_client, dwh_config, iamArn
    )

    # 1.4. Add some cluster props to configuration file
    dwh_config.set("DWH", "DWH_ENDPOINT", cluster_props["Endpoint"]["Address"])
    dwh_config.set("DWH", "DWH_ROLE_ARN", cluster_props["IamRoles"][0]["IamRoleArn"])
    dwh_config.set("DWH", "DWH_VPC_ID", cluster_props["VpcId"])

    with Path(__file__).parents[1].joinpath("dwh.cfg").open("w") as fp:
        dwh_config.write(fp)

    # 1.5. Open TCP port
    open_db_port(user_config, dwh_config)

    # 1.6. Drop/create empty tables
    conn, cur = get_db_connection(dwh_config)
    try:
        drop_tables(cur, conn)
        create_tables(cur, conn)
    except psycopg2.Error as e:
        logging.error(f"Error dropping/creating tables: \n{e}")
    conn.close()

    # 2. Airflow
    # 2.1. Save AWS connection
    register_connection(
        conn_id="aws_credentials",
        conn_type="Amazon Web Services",
        login=user_config.get("AWS", "AWS_ACCESS_KEY_ID"),
        password=user_config.get("AWS", "AWS_SECRET_ACCESS_KEY"),
    )

    # 2.2. Save Redshift connection
    register_connection(
        conn_id="aws_redshift",
        conn_type="Amazon Redshift",
        host=dwh_config.get("DWH", "DWH_ENDPOINT"),
        schema=dwh_config.get("DWH", "DWH_DB"),
        login=dwh_config.get("DWH", "DWH_DB_USER"),
        password=dwh_config.get("DWH", "DWH_DB_PASSWORD"),
        port=dwh_config.get("DWH", "DWH_DB_PORT"),
    )

    return cluster_props, redshift_client, iam_client


if __name__ == "__main__":
    main()
