import typing


DEFAULT_CONCURRENCY_LIMIT: typing.Final = 10
DEFAULT_COMMIT_BATCH_SIZE: typing.Final = 10
DEFAULT_COMMIT_BATCH_TIMEOUT_SEC: typing.Final = 10.0
DEFAULT_SHUTDOWN_TIMEOUT_SEC: typing.Final = 20.0
PROCESSING_CONTEXT_KEY: typing.Final = "concurrent_processing"
