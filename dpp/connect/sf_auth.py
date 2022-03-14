import requests
from typing import Tuple


def get_sf_credentials(
    token: str, base_sf_auth_url: str, duration: int = 1
) -> Tuple[str, str]:
    headers = {
        "accept": "application/hal+json",
        "Authorization": f"Bearer {token}",
    }
    body = {"duration": str(duration)}
    response = requests.request(
        method="POST", url=base_sf_auth_url, headers=headers, data=body
    )
    response.raise_for_status()
    content = response.json()["snowflake"]
    return content["username"], content["password"]
