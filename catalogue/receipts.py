"""Receipt identity, input framing and committed receipt access."""
from dataclasses import dataclass
import hashlib
import json


def valid_id(value):
    if not isinstance(value, str) or not value or '\x00' in value:
        raise ValueError('ID must be a nonempty string without NUL')
    return value


def frame(hasher, value):
    raw = json.dumps(value, ensure_ascii=True, separators=(',', ':')).encode('ascii')
    hasher.update(len(raw).to_bytes(8, 'big'))
    hasher.update(raw)


def new_manifest():
    return hashlib.sha256(b'catalogue-manifest-v1\x00')


def add_manifest(hasher, sequence, source_order, record_id, size, digest):
    frame(hasher, [sequence, source_order, record_id, size, digest])


@dataclass(frozen=True)
class Receipt:
    batch_id: str
    input_sha256: str
    generation: int
    record_count: int
    total_bytes: int
    manifest_sha256: str


def receipt(connection, batch_id):
    valid_id(batch_id)
    row = connection.execute(
        'SELECT batch_id,input_sha256,generation,record_count,total_bytes,'
        'manifest_sha256 FROM batches WHERE batch_id=?', (batch_id,)
    ).fetchone()
    return None if row is None else Receipt(*row)
