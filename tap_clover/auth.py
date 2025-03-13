"""Clover Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class CloverAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Clover."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the AutomaticTestTap API.

        Returns:
            A dict with the request body
        """
        return {
            "merchant_id": self.config.get("merchant_id"),
            "client_id": self.config.get("client_id"),
            "client_secret": self.config.get("client_secret"),
        }

    @classmethod
    def create_for_stream(cls, stream) -> CloverAuthenticator:  # noqa: ANN001
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        base_url = "https://api.clover.com"
        return cls(
            stream=stream,
            auth_endpoint=f"{base_url}/oauth/v2/token",
        )
