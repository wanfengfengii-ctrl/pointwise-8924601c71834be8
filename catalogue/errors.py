"""Public failure categories for catalogue ingestion and reading."""


class StructureError(ValueError):
    """An event stream is not a complete catalogue."""


class BatchConflict(ValueError):
    """A committed batch ID was reused with different validated input."""


class IntegrityError(ValueError):
    """Stored records disagree with their committed receipt."""
