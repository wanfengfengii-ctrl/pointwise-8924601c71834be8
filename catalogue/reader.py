"""Owned read cursors with byte, sequence and receipt integrity checks."""
from dataclasses import dataclass
import hashlib
import sqlite3

from .errors import IntegrityError
from .receipts import add_manifest, new_manifest, receipt, valid_id


@dataclass(frozen=True)
class ArchivedRecord:
    sequence: int
    source_order: int
    id: str
    body: bytes
    sha256: str


class Records:
    def __init__(self, dsn, uri, batch_id):
        valid_id(batch_id)
        self.closed = False
        self._cursor = None
        self._connection = sqlite3.connect(dsn, uri=uri, isolation_level=None)
        self._count = self._bytes = 0
        self._last_order = -1
        self._manifest = new_manifest()
        try:
            self._connection.execute('PRAGMA query_only=ON')
            self._connection.execute('BEGIN')
            self.receipt = receipt(self._connection, batch_id)
            if self.receipt is None:
                raise KeyError(batch_id)
            self._cursor = self._connection.execute(
                'SELECT sequence,source_order,record_id,body,sha256 FROM records '
                'WHERE batch_id=? ORDER BY sequence', (batch_id,))
        except BaseException:
            self.close()
            raise

    def __iter__(self):
        return self

    def __next__(self):
        if self.closed:
            raise StopIteration
        try:
            row = self._cursor.fetchone()
            if row is None:
                if (self._count != self.receipt.record_count
                        or self._bytes != self.receipt.total_bytes
                        or self._manifest.hexdigest() != self.receipt.manifest_sha256):
                    raise IntegrityError('receipt manifest mismatch')
                self.close()
                raise StopIteration
            item = ArchivedRecord(*row)
            if (type(item.sequence) is not int or type(item.source_order) is not int
                    or item.sequence != self._count or item.source_order <= self._last_order
                    or not isinstance(item.body, bytes)
                    or hashlib.sha256(item.body).hexdigest() != item.sha256):
                raise IntegrityError('record integrity mismatch')
            add_manifest(self._manifest, item.sequence, item.source_order,
                         item.id, len(item.body), item.sha256)
            self._count += 1
            self._bytes += len(item.body)
            self._last_order = item.source_order
            return item
        except BaseException:
            self.close()
            raise

    def close(self):
        if not self.closed:
            self.closed = True
            if self._cursor is not None:
                self._cursor.close()
            self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
