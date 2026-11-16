"""Offline JSON Lines source, yielding the public single-pass event protocol."""
import base64
import binascii
import json
from pathlib import Path

from .errors import StructureError


def open_source(path, *, chunk_size=65536):
    """Return an owned iterator. Only advance() opens the file; close() releases it."""
    if type(chunk_size) is not int or chunk_size <= 0:
        raise ValueError('chunk_size must be a positive integer')
    path = Path(path)

    def generate():
        with path.open('r', encoding='utf-8') as handle:
            for number, line in enumerate(handle, 1):
                try:
                    item = json.loads(line)
                    if not isinstance(item, dict):
                        raise ValueError('line must be an object')
                    kind = item.get('event')
                    fields = {
                        'begin_group': {'event', 'id'}, 'begin_record': {'event', 'id'},
                        'body': {'event', 'data'}, 'end_record': {'event', 'approved'},
                        'verdict': {'event', 'approved'}, 'end_group': {'event'},
                        'end_batch': {'event'}
                    }
                    if not isinstance(kind, str) or kind not in fields or set(item) != fields[kind]:
                        raise ValueError('unknown event or fields')
                    if kind == 'body':
                        if not isinstance(item['data'], str):
                            raise ValueError('body data must be base64 text')
                        raw = base64.b64decode(item['data'], validate=True)
                        if not raw:
                            yield ('body', b'')
                        else:
                            for offset in range(0, len(raw), chunk_size):
                                yield ('body', raw[offset:offset + chunk_size])
                    elif kind in ('begin_group', 'begin_record'):
                        yield (kind, item['id'])
                    elif kind in ('end_record', 'verdict'):
                        yield (kind, item['approved'])
                    else:
                        yield (kind,)
                except (ValueError, TypeError, binascii.Error) as exc:
                    raise StructureError('source line ' + str(number) + ': ' + str(exc)) from exc
    return generate()
