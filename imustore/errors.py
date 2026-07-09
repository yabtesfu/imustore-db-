class ImmuStoreError(Exception):
    """Base exception for ImmuStore-specific failures."""


class DatabaseClosedError(ValueError, ImmuStoreError):
    """Raised when a caller uses a closed database handle."""


class DatabaseCorruptionError(ImmuStoreError):
    """Raised when a storage record cannot be decoded safely."""


class KeyEncodingError(ImmuStoreError):
    """Raised when a database key is not a comparable string."""


class ConflictError(ImmuStoreError):
    """Raised when an optimistic transaction conflicts with a concurrent commit."""
