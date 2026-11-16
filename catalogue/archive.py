"""SQLite archive v1: whole-batch materialization followed by atomic publication."""
from dataclasses import dataclass
import hashlib
import sqlite3
import uuid

from .errors import BatchConflict
from .qualification import ingest_batch
from .receipts import Receipt, add_manifest, new_manifest, receipt, valid_id

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches(
 batch_id TEXT PRIMARY KEY,
 input_sha256 TEXT NOT NULL,
 generation INTEGER NOT NULL UNIQUE,
 record_count INTEGER NOT NULL,
 total_bytes INTEGER NOT NULL,
 manifest_sha256 TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS records(
 batch_id TEXT NOT NULL REFERENCES batches(batch_id),
 sequence INTEGER NOT NULL,
 source_order INTEGER NOT NULL,
 record_id TEXT NOT NULL,
 body BLOB NOT NULL,
 sha256 TEXT NOT NULL,
 PRIMARY KEY(batch_id,sequence),
 UNIQUE(batch_id,record_id)
);
CREATE TABLE IF NOT EXISTS current_batch(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 batch_id TEXT NOT NULL REFERENCES batches(batch_id)
);
PRAGMA user_version=1;
"""


@dataclass(frozen=True)
class PreparedRecord:
    sequence: int
    source_order: int
    record_id: str
    body: bytes
    sha256: str


def prepare(directory, observer):
    rows = []
    manifest = new_manifest()
    total = 0
    for node in directory.accepted_records():
        body = bytes(node.body)
        item = PreparedRecord(len(rows), node.source_order, node.id, body,
                              hashlib.sha256(body).hexdigest())
        rows.append(item)
        total += len(body)
        add_manifest(manifest, item.sequence, item.source_order, item.record_id,
                     len(body), item.sha256)
        if observer is not None:
            observer('body_staged', item.sequence)
    return rows, total, manifest.hexdigest()


class Archive:
    """One sequential writer; independent readers see only committed batches.

    observer(phase, detail) may inspect progress or raise an exception.
    Phases: body_staged(sequence), record_written(sequence), before_commit(receipt).
    Archive owns its connection, not the caller's database path.
    """

    def __init__(self, path, *, observer=None):
        self._closed = False
        self._uri = str(path) == ':memory:'
        self._dsn = ('file:catalogue-' + uuid.uuid4().hex + '?mode=memory&cache=shared'
                     if self._uri else str(path))
        self.observer = observer
        self._connection = sqlite3.connect(self._dsn, uri=self._uri, isolation_level=None)
        try:
            self._connection.execute('PRAGMA foreign_keys=ON')
            version = self._connection.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1):
                raise ValueError('unsupported archive schema')
            self._connection.executescript(SCHEMA)
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def _check_open(self):
        if self._closed:
            raise ValueError('archive is closed')

    def ingest_batch(self, batch_id, events):
        self._check_open()
        return ingest_batch(self, batch_id, events)

    def publish(self, batch_id, directory):
        """Publish a validated Directory; ingest_batch is the event-facing API."""
        self._check_open()
        valid_id(batch_id)
        rows, total, manifest = prepare(directory, self.observer)
        connection = self._connection
        connection.execute('BEGIN IMMEDIATE')
        try:
            old = receipt(connection, batch_id)
            if old is not None:
                if old.input_sha256 != directory.input_sha256:
                    raise BatchConflict('batch ID already has different input')
                connection.execute('ROLLBACK')
                return old
            generation = connection.execute(
                'SELECT COALESCE(MAX(generation),0)+1 FROM batches'
            ).fetchone()[0]
            result = Receipt(batch_id, directory.input_sha256, generation,
                             len(rows), total, manifest)
            connection.execute('INSERT INTO batches VALUES(?,?,?,?,?,?)',
                               tuple(result.__dict__.values()))
            for row in rows:
                connection.execute('INSERT INTO records VALUES(?,?,?,?,?,?)',
                                   (batch_id, row.sequence, row.source_order,
                                    row.record_id, row.body, row.sha256))
                if self.observer is not None:
                    self.observer('record_written', row.sequence)
            connection.execute(
                'INSERT INTO current_batch VALUES(1,?) '
                'ON CONFLICT(singleton) DO UPDATE SET batch_id=excluded.batch_id',
                (batch_id,))
            if self.observer is not None:
                self.observer('before_commit', result)
            connection.execute('COMMIT')
            return result
        except BaseException:
            if connection.in_transaction:
                connection.execute('ROLLBACK')
            raise

    def receipt(self, batch_id):
        self._check_open()
        return receipt(self._connection, batch_id)

    def current(self):
        self._check_open()
        row = self._connection.execute(
            'SELECT batch_id FROM current_batch WHERE singleton=1'
        ).fetchone()
        return None if row is None else self.receipt(row[0])

    def read_records(self, batch_id):
        self._check_open()
        from .reader import Records
        return Records(self._dsn, self._uri, batch_id)

    def close(self):
        if not self._closed:
            self._connection.close()
            self._closed = True

    def __enter__(self):
        self._check_open()
        return self

    def __exit__(self, *exc):
        self.close()
