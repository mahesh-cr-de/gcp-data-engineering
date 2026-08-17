"""Application-specific exception hierarchy."""


class ExportFrameworkError(Exception):
    """Base exception for expected framework failures."""


class ValidationError(ExportFrameworkError):
    """Raised when a request is invalid."""


class QueryExecutionError(ExportFrameworkError):
    """Raised when BigQuery execution or result streaming fails."""


class StorageError(ExportFrameworkError):
    """Raised when a Cloud Storage operation fails."""


class SFTPError(ExportFrameworkError):
    """Raised when an SFTP operation fails."""


class SecretResolutionError(ExportFrameworkError):
    """Raised when an SFTP secret cannot be resolved."""


class ExportTimeoutError(ExportFrameworkError):
    """Raised when the request's execution deadline is exhausted."""
