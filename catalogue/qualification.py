"""Single-pass validation and disk-backed staging of qualified records.

The parser keeps only the active record and the active group nesting stack in
Python memory.  Bodies are appended to an owned scratch file while a small
SQLite scratch database records the structure (group parent links, record
identities and hashes).  Ancestor eligibility is resolved from that scratch
state after the whole stream has been validated, so a late outer verdict never
requires a materialised tree or the batch's bodies to be resident.
"""
from dataclasses import dataclass
import hashlib
import os
import sqlite3
import tempfile

from .errors import IntegrityError, StructureError
from .receipts import frame, valid_id

_ARITIES = {'begin_group': 2, 'begin_record': 2, 'body': 2,
            'end_record': 2, 'verdict': 2, 'end_group': 1, 'end_batch': 1}

#: Bounded unit for staging reads and archive writes.
STAGE_CHUNK = 65536
_STAGE_SCHEMA = (
    'CREATE TABLE groups('
    ' idx INTEGER PRIMARY KEY, parent INTEGER, approved INTEGER)',
    'CREATE TABLE group_effective('
    ' idx INTEGER PRIMARY KEY, effective INTEGER NOT NULL)',
    'CREATE TABLE records('
    ' source_order INTEGER PRIMARY KEY, record_id BLOB NOT NULL UNIQUE,'
    ' offset INTEGER NOT NULL, size INTEGER NOT NULL, sha256 TEXT NOT NULL,'
    ' approved INTEGER NOT NULL, parent INTEGER)',
)


@dataclass(frozen=True)
class StagedRecord:
    """One accepted occurrence; the body remains in the scratch file."""
    source_order: int
    record_id: str
    offset: int
    size: int
    sha256: str


class Staging:
    """Owned scratch state for one ingestion.

    The body file is append-only during validation and is read back in bounded
    chunks during publication.  The metadata database is removed together with
    the body file by close(), whether the round succeeded, was rejected or
    raised.  Neither side ever holds a whole-batch body list, record-ID set or
    complete catalogue tree.
    """

    def __init__(self):
        self._closed = False
        self._finalized = False
        self._offset = 0
        self._group_count = 0
        self.input_sha256 = None
        self._body_path = self._meta_path = None
        self._body = self._db = None
        body_fd = meta_fd = None
        try:
            body_fd, self._body_path = tempfile.mkstemp(prefix='catalogue-stage-body-')
            self._body = os.fdopen(body_fd, 'w+b')
            body_fd = None
            meta_fd, self._meta_path = tempfile.mkstemp(prefix='catalogue-stage-meta-')
            os.close(meta_fd)
            meta_fd = None
            self._db = sqlite3.connect(self._meta_path, isolation_level=None)
            self._db.execute('PRAGMA journal_mode=OFF')
            self._db.execute('PRAGMA synchronous=OFF')
            for statement in _STAGE_SCHEMA:
                self._db.execute(statement)
            self._db.execute('BEGIN')
        except BaseException:
            for handle in (body_fd, meta_fd):
                if handle is not None:
                    try:
                        os.close(handle)
                    except OSError:
                        pass
            self.close()
            raise

    def _check_open(self):
        if self._closed:
            raise ValueError('staging is closed')

    def begin_group(self, parent):
        self._check_open()
        index = self._group_count
        self._group_count += 1
        self._db.execute('INSERT INTO groups VALUES(?,?,NULL)', (index, parent))
        return index

    def set_group_verdict(self, index, approved):
        self._check_open()
        self._db.execute('UPDATE groups SET approved=? WHERE idx=?',
                         (1 if approved else 0, index))

    def begin_record(self, source_order, record_id, parent):
        self._check_open()
        try:
            self._db.execute(
                'INSERT INTO records VALUES(?,?,?,0,?,0,?)',
                (source_order, record_id.encode('utf-8', 'surrogatepass'),
                 self._offset, '', parent))
        except sqlite3.IntegrityError as exc:
            raise StructureError('duplicate record ID') from exc

    def append_body(self, data):
        """Write one source chunk to the scratch body file."""
        self._check_open()
        self._body.write(data)
        self._offset += len(data)

    def end_record(self, source_order, size, digest, approved):
        self._check_open()
        self._db.execute(
            'UPDATE records SET size=?, sha256=?, approved=? WHERE source_order=?',
            (size, digest, 1 if approved else 0, source_order))

    def finalize(self):
        """Resolve every group's inherited eligibility, then seal the scratch state."""
        self._check_open()
        self._body.flush()
        cursor = self._db.execute('SELECT idx,parent,approved FROM groups ORDER BY idx')
        writer = self._db.cursor()
        reader = self._db.cursor()
        try:
            for index, parent, approved in cursor:
                if parent is None:
                    effective = approved
                else:
                    row = reader.execute(
                        'SELECT effective FROM group_effective WHERE idx=?',
                        (parent,)).fetchone()
                    effective = 1 if approved and row is not None and row[0] else 0
                writer.execute('INSERT INTO group_effective VALUES(?,?)',
                               (index, effective))
        finally:
            cursor.close()
            writer.close()
            reader.close()
        self._db.execute('COMMIT')
        self._finalized = True

    def accepted(self):
        """Yield accepted occurrences in source order without materialising them."""
        self._check_open()
        if not self._finalized:
            raise ValueError('staging is not finalized')
        cursor = self._db.execute(
            'SELECT r.source_order,r.record_id,r.offset,r.size,r.sha256 '
            'FROM records AS r LEFT JOIN group_effective AS g ON g.idx=r.parent '
            'WHERE r.approved=1 AND (r.parent IS NULL OR g.effective=1) '
            'ORDER BY r.source_order')
        try:
            for source_order, record_id, offset, size, digest in cursor:
                yield StagedRecord(source_order,
                                   record_id.decode('utf-8', 'surrogatepass'),
                                   offset, size, digest)
        finally:
            cursor.close()

    def chunks(self, record, width=65536):
        """Yield one staged body in bounded pieces, verifying its extent."""
        self._check_open()
        self._body.seek(record.offset)
        remaining = record.size
        while remaining:
            data = self._body.read(min(width, remaining))
            if not data:
                raise IntegrityError('staged body is truncated')
            remaining -= len(data)
            yield data

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._body is not None:
            try:
                self._body.close()
            except BaseException:
                pass
        if self._db is not None:
            try:
                self._db.close()
            except BaseException:
                pass
        for path in (self._body_path, self._meta_path):
            if path is not None:
                try:
                    os.unlink(path)
                except OSError:
                    # Cleanup must never replace the failure that caused it.
                    pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class _ActiveRecord:
    __slots__ = ('source_order', 'digest', 'size')

    def __init__(self, source_order):
        self.source_order = source_order
        self.digest = hashlib.sha256()
        self.size = 0


def parse(events):
    """Validate a complete single-pass catalogue and return owned Staging.

    The staging is cleaned up here if validation fails; on success the caller
    owns it and must close it (the archive's ingest_batch does so in a finally
    after publication or failure).
    """
    staging = Staging()
    try:
        active = None
        stack = []
        order = 0
        ended = False
        fingerprint = hashlib.sha256(b'catalogue-input-v1\x00')
        for event in events:
            if ended:
                raise StructureError('event after end_batch')
            if not isinstance(event, tuple) or not event or not isinstance(event[0], str):
                raise StructureError('events must be nonempty tuples')
            kind = event[0]
            if kind not in _ARITIES or len(event) != _ARITIES[kind]:
                raise StructureError('unknown event or invalid arity')
            if kind in ('begin_group', 'begin_record'):
                if active is not None:
                    raise StructureError('nested event inside record')
                try:
                    valid_id(event[1])
                except ValueError as exc:
                    raise StructureError(str(exc)) from exc
                if kind == 'begin_group':
                    parent = stack[-1][0] if stack else None
                    index = staging.begin_group(parent)
                    stack.append([index, None])
                else:
                    parent = stack[-1][0] if stack else None
                    staging.begin_record(order, event[1], parent)
                    active = _ActiveRecord(order)
                    order += 1
                frame(fingerprint, list(event))
            elif kind == 'body':
                if active is None or type(event[1]) is not bytes:
                    raise StructureError('body requires an open record and bytes')
                staging.append_body(event[1])
                active.digest.update(event[1])
                active.size += len(event[1])
            elif kind == 'end_record':
                if active is None or type(event[1]) is not bool:
                    raise StructureError('end_record requires a boolean verdict')
                digest = active.digest.hexdigest()
                staging.end_record(active.source_order, active.size, digest, event[1])
                frame(fingerprint, [kind, event[1], active.size, digest])
                active = None
            elif kind == 'verdict':
                if active is not None or not stack or type(event[1]) is not bool:
                    raise StructureError('verdict requires an open group and bool')
                if stack[-1][1] is not None:
                    raise StructureError('duplicate group verdict')
                stack[-1][1] = event[1]
                staging.set_group_verdict(stack[-1][0], event[1])
                frame(fingerprint, list(event))
            elif kind == 'end_group':
                if active is not None or not stack or stack[-1][1] is None:
                    raise StructureError('group is missing its final verdict')
                stack.pop()
                frame(fingerprint, [kind])
            else:
                if active is not None or stack:
                    raise StructureError('unclosed catalogue at end_batch')
                ended = True
                frame(fingerprint, [kind])
        if not ended:
            raise StructureError('missing end_batch')
        staging.input_sha256 = fingerprint.hexdigest()
        staging.finalize()
        return staging
    except BaseException:
        staging.close()
        raise


def ingest_batch(archive, batch_id, events):
    """Own and close the event iterator; publish only after EOF and close succeed.

    The event iterator is closed before any publication begins.  The staging
    resources are released on every exit path, including source failures,
    staging faults, observer exceptions and publication rollback.
    """
    valid_id(batch_id)
    iterator = iter(events)
    close = getattr(iterator, 'close', None)
    try:
        staging = parse(iterator)
    except BaseException:
        if close is not None:
            try:
                close()
            except BaseException:
                pass
        raise
    try:
        if close is not None:
            close()
        return archive.publish(batch_id, staging)
    finally:
        staging.close()
