import json
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import Stream as RESTStreamBase
import backoff


class EmptyResponseError(Exception):
    """Raised when the response is empty"""


class CloverAuthenticator(OAuthAuthenticator):
    def __init__(
            self,
            stream: RESTStreamBase,
            config_file: Optional[str] = None,
            auth_endpoint: Optional[str] = None,
    ) -> None:
        super().__init__(stream=stream)
        self._auth_endpoint = "https://sandbox.dev.clover.com/oauth/v2/refresh"
        self._config_file = config_file
        self._tap = stream._tap

    @property
    def auth_headers(self) -> dict:
        """Return a dictionary of auth headers to be applied.

        These will be merged with any `http_headers` specified in the stream.

        Returns:
            HTTP headers for authentication.
        """
        if not self.is_token_valid():
            self.update_access_token()
        result = {}
        result["Authorization"] = f"Bearer {self._tap._config.get('access_token')}"
        return result

    @auth_headers.setter
    def auth_headers(self, value: dict) -> None:
        self.__dict__['_auth_headers'] = value

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the clover API."""
        return {
            "refresh_token": self._tap._config["refresh_token"],
            "client_id": self._tap._config["client_id"]
        }

    def is_token_valid(self) -> bool:
        access_token = self._tap._config.get("access_token")
        now = round(datetime.utcnow().timestamp())
        expires_in = self._tap.config.get("expires_in")
        if expires_in is not None:
            expires_in = int(expires_in)
        if not access_token:
            return False

        if not expires_in:
            return False

        return not ((expires_in - now) < 120)

    @backoff.on_exception(backoff.expo, EmptyResponseError, max_tries=5, factor=2)
    def update_access_token(self) -> None:
        headers = {"Content-Type": "application/json"}
        token_response = requests.post(
            self._auth_endpoint, json=self.oauth_request_body, headers=headers
        )
        try:
            if (
                    token_response.json().get("error_description")
                    == "Rate limit exceeded: access_token not expired"
            ):
                return None
        except Exception as e:
            raise EmptyResponseError(f"Failed converting response to a json, because response is empty")

        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        # Log the refresh_token
        self.logger.info(f"Latest refresh token: {token_json['refresh_token']}")

        self.access_token = token_json["access_token"]

        self._tap._config["access_token"] = token_json["access_token"]
        self._tap._config["refresh_token"] = token_json["refresh_token"]
        now = round(datetime.utcnow().timestamp())
        self._tap._config["expires_in"] = int(token_json["access_token_expiration"]) + now

        with open(self._tap.config_file, "w") as outfile:
            json.dump(self._tap._config, outfile, indent=4)
