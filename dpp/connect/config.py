import configparser


class Config:
    def __init__(self, account_name, client_id):
        endpoint_urls = configparser.ConfigParser()
        endpoint_urls.read("dpp/config.ini")
        self.audience_url = endpoint_urls["url"]["AUDIENCE_URL"]
        self.oauth_url = endpoint_urls["url"]["OAUTH_URL"]
        self.base_sf_auth_url = (
            endpoint_urls["url"]["BASE_SF_AUTH_URL"]
            + f"/v0/accounts/{account_name}/users/{client_id}@clients/credentials"
        )

    def get_config(self):
        return {
            "audience_url": self.audience_url,
            "oauth_url": self.oauth_url,
            "base_sf_auth_url": self.base_sf_auth_url,
        }
