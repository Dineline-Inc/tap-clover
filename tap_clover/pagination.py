"""REST API pagination handling."""

from singer_sdk.pagination import BaseOffsetPaginator


class CustomOffsetPaginator(BaseOffsetPaginator):

    def get_next(self, response):
        try:
            json_response = response.json()
            elements = json_response.get("elements")
            if elements:
                return self._value + self._page_size
        except Exception:
            return None
        return None
