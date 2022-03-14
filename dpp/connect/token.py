import json
import requests
import datetime


class Token:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        audience_url: str,
        oauth_url: str,
    ):
        self._client_id = client_id
        self._client_secret = client_secret
        self._audience_url = audience_url
        self._oauth_url = oauth_url
        self._token = None
        self._expires_at = datetime.datetime.utcnow()

    def get_token(self):
        if not self._token or self._is_expired():
            headers = {
                "content-type": "application/json",
            }
            data = {
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "audience": self._audience_url,
            }
            data = json.dumps(data)
            response_token = requests.post(
                self._oauth_url, headers=headers, data=data,
            )

            response_token.raise_for_status()

            self._token = response_token.json().get("access_token")
            self._set_new_expired_datetime(
                response_token.json().get("expires_in")
            )

        return self._token

    def _is_expired(self):
        return self._expires_at < datetime.datetime.utcnow()

    def _set_new_expired_datetime(self, seconds):
        self._expires_at = datetime.datetime.utcnow() + datetime.timedelta(
            seconds=seconds
        )
