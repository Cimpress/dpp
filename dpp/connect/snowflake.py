import re
from typing import Dict, Tuple
import warnings
from Cryptodome.IO import PEM
from dpp.connect.secrets import AwsSecret
from dpp.connect.sf_auth import get_sf_credentials
from dpp.connect.token import Token
from dpp.connect.config import Config
import logging

logger = logging.getLogger(__name__)


def _get_snowflake_authentication(
    client_id: str = None, client_secret: str = None, account_name: str = None,
) -> Dict:
    config = Config(account_name, client_id)
    config_values = config.get_config()
    audience_url, oauth_url, base_sf_auth_url = (
        config_values["audience_url"],
        config_values["oauth_url"],
        config_values["base_sf_auth_url"],
    )
    token = Token(
        client_id, client_secret, audience_url, oauth_url
    ).get_token()
    return base_sf_auth_url, token


def get_snowflake_options(
    database: str,
    schema: str,
    username: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    *,
    role: str = None,
    client_id: str = None,
    client_secret: str = None,
    account_name: str = "vistaprint",
    duration: int = 1,
) -> Dict:
    """
    Used to retrieve a Snowflake options dictionary. This dictionary is then passed to the
    snowflake.connector.connect function to create a snowflake connection.

    For more information about the snowflake python connector please see the snowflake official
    documentation.
    If you are using Auth0 Client keys to get credentials to authenticate with snowflake, you must use the following
    parameters (for Auth0 Client Keys you MUST use keyword parameters):
        * role
        * client_id
        * client_secret

    :param database: The name of the database that you will connect to
    :param schema: The name of the schema that you will connect to
    :param username: The snowflake user that you will use when querying or writing to snowflake
    :param aws_access_key_id: The aws access key id that you will use to get snowflake private key for your user
    :param aws_secret_access_key: The aws secret access key that you will use to get snowflake private key for your user
    :param role: The snowflake role that you will use when querying or writing to snowflake
    :param client_id: The client id that you will use to get snowflake credentials
    :param client_secret: The client secret that you will use to get snowflake credentials
    :param account_name: The Snowflake account name that you are connecting to
    :param duration: The duration in days that the credentials should last for. Valid range for a credential is from 1 day to 90 days

    :return: A snowflake options dictionary.

    :raises ValueError: If the correct arguments are not passed
    :raises requests.HTTPError: If there are problems connecting to the credentials services, either AWS or Auth0 Client

    :example:
    .. code-block::

        import snowflake.connector
        from dpp_connect import get_snowflake_options

        # For Auth0 Client Keys
        sf_options = get_snowflake_options("my_db_name", "my_schema_name",
                                            role = "my_role",
                                            client_id="my_client_id",
                                            client_secret="my_client_secret")
        sf_conn = snowflake.connector.connect(**sf_options)
    """

    key_type, key_id, secret = _get_keys(
        username,
        aws_access_key_id,
        aws_secret_access_key,
        role,
        client_id,
        client_secret,
    )

    result = {
        "account": f"{account_name}.eu-west-1",
        "database": database,
        "schema": schema,
        "warehouse": "PUBLIC",
    }

    if key_type == "AWS":
        username = username.upper()
        user_private_key = AwsSecret(key_id, secret).get(
            f"snowflake/{username}"
        )

        result["private_key"] = PEM.decode(user_private_key)[0]
        result["user"] = username

    if key_type == "auth0_client":
        base_sf_auth_url, token = _get_snowflake_authentication(
            client_id, client_secret, account_name
        )

        result["user"], result["password"] = get_sf_credentials(
            token, base_sf_auth_url, duration
        )
        result["role"] = role
    return result


def get_spark_snowflake_options(
    database: str,
    schema: str,
    username: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    *,
    role: str = None,
    client_id: str = None,
    client_secret: str = None,
    account_name: str = "vistaprint",
    duration: int = 1,
) -> Dict:
    """
    Used to retrieve a Snowflake options dictionary for connecting to snowflake from databricks.
    This dictionary is then used as options when creating a dataframe using spark.read.format("snowflake")

    For more information about the snowflake python connector please see the snowflake and/or databricks official
    documentation.

    If you are using AWS keys to get credentials to authenticate with snowflake, you must use the following
    parameters, either as positionl parameters or keyword parameters (it is recommended to use keyword parameters
    for clarity):
        * username
        * aws_access_key_id
        * aws_secret_access_key

    If you are using Client keys to get credentials to authenticate with snowflake, you must use the following
    parameters (for Client Keys you MUST use keyword parameters):
        * role
        * client_id
        * client_secret

    :param database: The name of the database that you will connect to
    :param schema: The name of the schema that you will connect to
    :param username: The snowflake user that you will use when querying or writing to snowflake
    :param aws_access_key_id: The aws access key id that you will use to get snowflake private key for your user
    :param aws_secret_access_key: The aws secret access key that you will use to get snowflake private key for your user
    :param role: The snowflake role that you will use when querying or writing to snowflake
    :param client_id: The client id that you will use to get snowflake credentials
    :param client_secret: The client secret that you will use to get snowflake credentials
    :param account_name: The Snowflake account name that you are connecting to
    :param duration: The duration in days that the credentials should last for. Valid range for a credential is from 1 day to 90 days

    :return: A snowflake options dictionary.

    :raises ValueError: If the correct arguments are not passed
    :raises requests.HTTPError: If there are problems connecting to the credentials services, either AWS or Auth0 Client

    :example:
    .. code-block::
        from dpp_connect import get_snowflake_options

        # For Client Keys
        sf_options = get_spark_snowflake_options("my_db_name", "my_schema_name",
                                                 role = "my_role",
                                                 client_id="my_client_id",
                                                 client_secret="my_client_secret")

        # For AWS Access Keys
        sf_options = get_spark_snowflake_options("my_db_name", "my_schema_name",
                                                 username = "my_username",
                                                 aws_access_key_id="my_aws_access_key_id",
                                                 aws_secret_access_key="my_aws_secret_access_key")

        # For AWS Access Keys using positional arguments
        sf_options = get_spark_snowflake_options("my_db_name", "my_schema_name", "my_username",
                                                 "my_aws_access_key_id", "my_aws_secret_access_key")

        df = (
        spark.read
          .format("snowflake")
          .options(**options)
          .option("query",  "select 1 as my_num union all select 2 as my_num")
          .load()
        )
    """

    key_type, key_id, secret = _get_keys(
        username,
        aws_access_key_id,
        aws_secret_access_key,
        role,
        client_id,
        client_secret,
    )
    result = {
        "sfURL": f"{account_name}.eu-west-1.snowflakecomputing.com",
        "sfDatabase": database,
        "sfSchema": schema,
        "sfWarehouse": "PUBLIC",
    }

    if key_type == "AWS":
        username = username.upper()
        private_key = AwsSecret(key_id, secret).get(f"snowflake/{username}")
        result["pem_private_key"] = re.sub(
            "-*(BEGIN|END) RSA PRIVATE KEY-*", "", private_key
        ).replace("\n", "")
        result["sfUser"] = username
        return result
    if key_type == "auth0_client":
        base_sf_auth_url, token = _get_snowflake_authentication(
            client_id, client_secret, account_name
        )
        result["sfUser"], result["sfPassword"] = get_sf_credentials(
            token, base_sf_auth_url, duration
        )
        result["sfRole"] = role
        return result


def _get_keys(
    username: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    role: str,
    client_id: str,
    client_secret: str,
) -> Tuple[str, str, str]:
    deprecation_msg = (
        "Support for using AWS keys to get credentials to authenticate with snowflake will soon be "
        "deprecated. Please switch to using Auth0 Client keys as soon as possible."
    )
    if (
        username is not None
        and aws_access_key_id is not None
        and aws_secret_access_key is not None
    ):
        warnings.warn(deprecation_msg, category=DeprecationWarning)
        return "AWS", aws_access_key_id, aws_secret_access_key
    elif (
        role is not None
        and client_id is not None
        and client_secret is not None
    ):
        return "auth0_client", client_id, client_secret
    else:
        raise ValueError(
            "You must use either 'username', 'aws_access_key_id' and 'aws_secret_access_key' as "
            "positional/keyword parameters or 'role', 'client_id' and 'client_secret' as keyword "
            "arguments"
        )
