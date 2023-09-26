import logging

from notion_client.errors import APIResponseError, RequestTimeoutError, HTTPResponseError
from tenacity import RetryCallState
from tenacity.wait import wait_base


logger = logging.getLogger(__name__)


def is_rate_limit_exception(exception: BaseException) -> bool:
    """Return True if the exception is a rate limit exception."""
    if isinstance(exception, APIResponseError):
        if exception.code == "rate_limited":
            wait_time = exception.headers.get("Retry-After")
            logger.warning(f"Rate limit exceeded. Waiting for {wait_time} seconds following Retry-After header.")
            return True

    return False


def is_unavailable_exception(exception: BaseException) -> bool:
    """Return True if the exception is an unavailable exception."""
    if isinstance(exception, APIResponseError):
        if exception.code in [
            "service_unavailable",
            "database_connection_unavailable",
            "gateway_timeout",
            "internal_server_error",
        ]:
            logger.warning("Service unavailable. Trying again.")
            return True
    if isinstance(exception, RequestTimeoutError):
        logger.warning("Request timeout. Trying again.")
        return True
    if isinstance(exception, HTTPResponseError):
        if exception.status == 504:
            logger.warning("Gateway timeout. Trying again.")
            return True

    return False


class wait_for_retry_after_header(wait_base):
    """Wait strategy that tries to wait for the length specified by
    the Retry-After header, or the underlying wait strategy if not.
    See RFC 6585 § 4.

    Otherwise, wait according to the fallback strategy.
    """

    def __init__(self, fallback: wait_base):
        self.fallback = fallback

    def __call__(self, retry_state: RetryCallState) -> float:
        # retry_state is an instance of tenacity.RetryCallState.  The .outcome
        # property is the result/exception that came from the underlying function.
        exc = retry_state.outcome.exception()
        if isinstance(exc, APIResponseError):
            retry_after = exc.headers.get("Retry-After")
            try:
                return int(retry_after)
            except (TypeError, ValueError):
                pass

        return self.fallback(retry_state)
