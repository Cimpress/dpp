import boto3
import json
import logging

logger = logging.getLogger(__name__)


class AwsSecret:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str):
        if aws_access_key_id is None or aws_secret_access_key is None:
            logger.error("Access Key and secret are required ")
            raise ValueError(
                f"{self.__class__.__name__}: Access Key and secret are required "
            )
        self.client = boto3.client(
            service_name="secretsmanager",
            region_name="eu-west-1",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def get(self, secret_name):
        try:
            secret_value = self.client.get_secret_value(SecretId=secret_name)

            if "SecretString" in secret_value:
                return json.loads(secret_value["SecretString"])["sf_account"]
            else:
                return secret_value["SecretBinary"].decode()

        except Exception as e:
            logger.error(e)
            raise e
