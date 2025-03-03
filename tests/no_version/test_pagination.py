import math

from pydantic import model_validator

from fractal_server.app.routes.pagination import PaginationRequest  # noqa
from fractal_server.app.routes.pagination import PaginationResponse


class PaginationResponseValidated(PaginationResponse):
    @model_validator(mode="after")
    def valid_page(self):

        if self.page_size == 0 and self.total_count > 0:
            raise ValueError(
                "'page_size' must be greater than 0 "
                "when 'total_count' is greater than 0."
            )
        if self.page_size > 0 and len(self.items) > self.page_size:
            raise ValueError(
                f"'items' list length ({len(self.items)}) "
                f"exceeds 'page_size' ({self.page_size})."
            )

        max_page = math.ceil(self.total_count / self.page_size)

        if self.current_page == max_page and self.page_size > 0:
            expected_items_on_last_page = self.total_count % self.page_size
            if expected_items_on_last_page == 0 and self.total_count > 0:
                expected_items_on_last_page = self.page_size
            if len(self.items) > expected_items_on_last_page:
                raise ValueError(
                    f"Too many items on the last page: got {len(self.items)}, "
                    f"expected at most {expected_items_on_last_page}."
                )
        if self.current_page > max_page and len(self.items) > 0:
            raise ValueError(
                f"Current page must be empty: got {len(self.items)} items."
            )

        return self
