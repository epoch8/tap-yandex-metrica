"""YandexMetrica Authentication."""

from __future__ import annotations

import sys

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class YandexMetricaAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for YandexMetrica."""

    @override
    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the YandexMetrica API.

        Returns:
            A dict with the request body
        """
        # TODO: Define the request body needed for the API.
        return {
            "resource": "https://example.com",
            "scope": self.oauth_scopes,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
