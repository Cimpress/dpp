## Introduction

dpp.connect enables users to gain programmatic access to Snowflake from their Python based data pipelines.

## Prerequisites

* Python 3.7 or higher

* A Client with access to a Snowflake Role

## Installation


```shell
pip install dpp
```

## Setting up config variables
You need to have an `config.ini` file to work with DPP

Name                   |Description                                                                                 | When To Use
-----------------------|--------------------------------------------------------------------------------------------|-----------------
`AUDIENCE_URL`             | The audience (presented as the aud claim in the access token) defines the intended recipients of the token. It can be added to the request to authorize i.e. audience: `https://test-api`                                      | Always
`OAUTH_URL` | The oauth url to send post request for getting access token. | Always
`BASE_SF_AUTH_URL` | The snowflake auth url to send post request for getting snowflake credentials | Always

## API and Examples

### get_snowflake_options
#### Signature
```python
def get_snowflake_options(
  database: str,
  schema: str,
  client_id: str = None,
  client_secret: str = None,
  account_name: str = None,
  duration: int = 1
) -> Dict:

```
#### Description
Used to retrieve a Snowflake options dictionary. This dictionary is then passed to the snowflake.connector.connect function to create a snowflake connection.

For more information about the Snowflake Python Connector please see the official [Snowflake documentation](https://docs.snowflake.com/en/user-guide/python-connector.html).

If you are using Domain Client keys to get credentials to authenticate with Snowflake, you must use the following parameters (for Domain Client Keys you MUST use keyword parameters):
* role
* client_id
* client_secret

#### Parameters
> **_IMPORTANT:_**  To improve the clarity of your code and to ensure that you are not affected by any future changes, always pass parameters as Keyword/Named Arguments

Name                   |Description                                                                                 | When To Use
-----------------------|--------------------------------------------------------------------------------------------|-----------------
`database`             | The name of the database that you will connect to                                          | Always
`schema`               | The name of the schema that you will connect to                                            | Always
`role`                 | The snowflake role that you will use when querying or writing to snowflake                 | With domain Client Keys
`client_id`            | The domain client id that you will use to get snowflake credentials                      | With domain Client Keys
`client_secret`        | The domain client secret that you will use to get snowflake credentials                  | With domain Client Keys
`account_name`         | The Snowflake account name that you are connecting to | Always
`duration`             | The duration in days that you would like the temporary Snowflake user to be available for  | Optional with domain Client Keys, defaults to 1

#### Return value

The function returns a Snowflake options dictionary.

#### Exceptions

Exception                  | When
---------------------------|-------------------------
`ValueError`               | The correct arguments are not passed
`requests.HTTPError`       | There are problems connecting to the credentials service, Auth0 Client

#### Example
For Client Keys:
```python
from dpp_connect import get_snowflake_options
import snowflake.connector

sf_options = get_snowflake_options(
    database="my_db_name",
    schema="my_schema_name",
    role="my_role",
    client_id="my_client_id",
    client_secret="my_client_secret",
    account_name="",
    duration=1
)

sf_conn = snowflake.connector.connect(**sf_options)

```

---

### get_spark_snowflake_options
#### Signature
```python
def get_spark_snowflake_options(
  database: str,
  schema: str,
  client_id: str = None,
  client_secret: str = None,
  account_name: str = None,
  duration: int = 1
) -> Dict:

```
#### Description
Used to retrieve a Snowflake options dictionary to be used in Spark context. This dictionary is then passed to the snowflake.connector.connect function to create a snowflake connection.

For more information about the Snowflake Python Connector please see the official [Snowflake documentation](https://docs.snowflake.com/en/user-guide/python-connector.html).

If you are using Domain Client keys to get credentials to authenticate with Snowflake, you must use the following parameters (for Domain Client Keys you MUST use keyword parameters):
* role
* client_id
* client_secret

#### Parameters
> **_IMPORTANT:_**  To improve the clarity of your code and to ensure that you are not affected by any future changes, always pass parameters as Keyword/Named Arguments

Name                   |Description                                                                                 | When To Use
-----------------------|--------------------------------------------------------------------------------------------|-----------------
`database`             | The name of the database that you will connect to                                          | Always
`schema`               | The name of the schema that you will connect to                                            | Always
`role`                 | The snowflake role that you will use when querying or writing to snowflake                 | With Domain Client Keys
`client_id`            | The domain client id that you will use to get snowflake credentials                      | With Domain Client Keys
`client_secret`        | The domain client secret that you will use to get snowflake credentials                  | With Domain Client Keys
`account_name`         | The Snowflake account name that you are connecting to | Always
`duration`             | The duration in days that you would like the temporary Snowflake user to be available for  | Optional with Domain Client Keys, defaults to 1

#### Return value

The function returns a Snowflake options dictionary.

#### Exceptions

Exception                  | When
---------------------------|-------------------------
`ValueError`               | The correct arguments are not passed
`requests.HTTPError`       | There are problems connecting to the credentials service, Auth0 Client

#### Example
For Client Keys:
```python
%pip install dpp
# COMMAND ----------
client_id = dbutils.secrets.get("dpp_sdk", "client_id")
client_secret = dbutils.secrets.get("dpp_sdk", "client_secret")

# COMMAND ----------
from dpp.connect import get_spark_snowflake_options
options = get_spark_snowflake_options(database="<database>",
                                      schema="<schema>",
                                      role="<role>",
                                      client_id=client_id,
                                      client_secret=client_secret)

# COMMAND ----------
# Create "test_table" and write 5 rows
spark.range(5).write.format("snowflake").options(**options).option("dbtable", "test_table").save()

# COMMAND ----------
# Read "test_table" and display on the screen
df = spark.read.format("snowflake").options(**options).option("dbtable", "test_table").load()
display(df)

# COMMAND ----------
# Clean up the "test_table"
sfUtils = sc._jvm.net.snowflake.spark.snowflake.Utils
sfUtils.runQuery(options, "DROP TABLE test_table")

```

