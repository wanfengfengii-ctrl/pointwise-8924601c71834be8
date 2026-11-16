"""SQLite archive v1: bounded staging followed by atomic publication."""
import sqlite3
import uuid

from .errors import BatchConflict, IntegrityError
from .qualification import STAGE_CHUNK, ingest_batch
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


def _insert_record(connection, batch_id, staging, record, sequence):
    """Insert one staged body without materialising the batch in Python.

    Bodies up to one bounded chunk are bound directly, so only that chunk is
    transiently resident.  Larger bodies reserve SQLite's own zeroblob storage
    and are filled through the incremental blob API one chunk at a time.  No
    path ever holds a record's whole body alongside another record's body, and
    no path holds the batch.
    """
    if record.size <= STAGE_CHUNK:
        piece = b''
        if record.size:
            piece = next(staging.chunks(record, STAGE_CHUNK), b'')
            if len(piece) != record.size:
                raise IntegrityError('staged body length mismatch')
        connection.execute(
            'INSERT INTO records VALUES(?,?,?,?,?,?)',
            (batch_id, sequence, record.source_order, record.record_id,
             piece, record.sha256))
        return
    cursor = connection.execute(
        'INSERT INTO records VALUES(?,?,?,?,zeroblob(?),?)',
        (batch_id, sequence, record.source_order, record.record_id,
         record.size, record.sha256))
    rowid = cursor.lastrowid
    target = connection.blobopen('records', 'body', rowid)
    try:
        offset = 0
        for piece in staging.chunks(record, STAGE_CHUNK):
            if not piece:
                continue
            target[offset:offset + len(piece)] = piece
            offset += len(piece)
        if offset != record.size:
            raise IntegrityError('staged body length mismatch')
    finally:
        target.close()


def prepare(staging, observer):
    """Metadata-only pass: totals, manifest and the body_staged progress port.

    It reads accepted identities from the scratch database without touching a
    single body byte, so it stays bounded regardless of batch size.
    """
    manifest = new_manifest()
    total = 0
    count = 0
    for record in staging.accepted():
        add_manifest(manifest, count, record.source_order, record.record_id,
                     record.size, record.sha256)
        total += record.size
        if observer is not None:
            observer('body_staged', count)
        count += 1
    return count, total, manifest.hexdigest()


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

    def publish(self, batch_id, staging):
        """Publish a validated Staging; ingest_batch is the event-facing API.

        Staging stays owned by ingest_batch, which closes it in a finally; this
        method only consumes it.  Nothing is committed unless every staged body
        was written and the observer allowed the round to continue.
        """
        self._check_open()
        valid_id(batch_id)
        # Metadata-only pass first: the original implementation emitted
        # body_staged before opening the publication transaction, including for
        # replays and conflicts.  Keep that observable ordering.
        count, total, manifest = prepare(staging, self.observer)
        connection = self._connection
        connection.execute('BEGIN IMMEDIATE')
        try:
            old = receipt(connection, batch_id)
            if old is not None:
                if old.input_sha256 != staging.input_sha256:
                    raise BatchConflict('batch ID already has different input')
                connection.execute('ROLLBACK')
                return old
            generation = connection.execute(
                'SELECT COALESCE(MAX(generation),0)+1 FROM batches'
            ).fetchone()[0]
            result = Receipt(batch_id, staging.input_sha256, generation,
                             count, total, manifest)
            connection.execute('INSERT INTO batches VALUES(?,?,?,?,?,?)',
                               tuple(result.__dict__.values()))
            sequence = 0
            for record in staging.accepted():
                _insert_record(connection, batch_id, staging, record, sequence)
                if self.observer is not None:
                    self.observer('record_written', sequence)
                sequence += 1
            if sequence != count:
                raise IntegrityError('staging changed during publication')
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
