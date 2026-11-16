"""Complete-tree validation and qualification of single-pass event streams."""
from dataclasses import dataclass, field
import hashlib

from .errors import StructureError
from .receipts import frame, valid_id


@dataclass
class Group:
    id: str
    children: list = field(default_factory=list)
    approved: object = None


@dataclass
class Record:
    id: str
    source_order: int
    body: bytearray = field(default_factory=bytearray)
    approved: object = None


@dataclass
class Directory:
    roots: list
    input_sha256: str

    def accepted_records(self):
        stack = [(iter(self.roots), True)]
        while stack:
            children, eligible = stack[-1]
            node = next(children, None)
            if node is None:
                stack.pop()
            elif isinstance(node, Group):
                stack.append((iter(node.children), eligible and node.approved))
            elif eligible and node.approved:
                yield node


def parse(events):
    roots, groups, record_ids = [], [], set()
    active = None
    order = 0
    ended = False
    fingerprint = hashlib.sha256(b'catalogue-input-v1\x00')
    for event in events:
        if ended:
            raise StructureError('event after end_batch')
        if not isinstance(event, tuple) or not event or not isinstance(event[0], str):
            raise StructureError('events must be nonempty tuples')
        kind = event[0]
        arities = {'begin_group': 2, 'begin_record': 2, 'body': 2,
                   'end_record': 2, 'verdict': 2, 'end_group': 1, 'end_batch': 1}
        if kind not in arities or len(event) != arities[kind]:
            raise StructureError('unknown event or invalid arity')
        if kind in ('begin_group', 'begin_record'):
            if active is not None:
                raise StructureError('nested event inside record')
            try:
                valid_id(event[1])
            except ValueError as exc:
                raise StructureError(str(exc)) from exc
            destination = groups[-1].children if groups else roots
            if kind == 'begin_group':
                node = Group(event[1])
                destination.append(node)
                groups.append(node)
            else:
                if event[1] in record_ids:
                    raise StructureError('duplicate record ID')
                record_ids.add(event[1])
                active = Record(event[1], order)
                order += 1
                destination.append(active)
            frame(fingerprint, list(event))
        elif kind == 'body':
            if active is None or type(event[1]) is not bytes:
                raise StructureError('body requires an open record and bytes')
            active.body.extend(event[1])
        elif kind == 'end_record':
            if active is None or type(event[1]) is not bool:
                raise StructureError('end_record requires a boolean verdict')
            active.approved = event[1]
            frame(fingerprint, [kind, event[1], len(active.body),
                                hashlib.sha256(active.body).hexdigest()])
            active = None
        elif kind == 'verdict':
            if active is not None or not groups or type(event[1]) is not bool:
                raise StructureError('verdict requires an open group and bool')
            if groups[-1].approved is not None:
                raise StructureError('duplicate group verdict')
            groups[-1].approved = event[1]
            frame(fingerprint, list(event))
        elif kind == 'end_group':
            if active is not None or not groups or groups[-1].approved is None:
                raise StructureError('group is missing its final verdict')
            groups.pop()
            frame(fingerprint, [kind])
        else:
            if active is not None or groups:
                raise StructureError('unclosed catalogue at end_batch')
            ended = True
            frame(fingerprint, [kind])
    if not ended:
        raise StructureError('missing end_batch')
    return Directory(roots, fingerprint.hexdigest())


def ingest_batch(archive, batch_id, events):
    """Own and close the event iterator; publish only after EOF and close succeed."""
    valid_id(batch_id)
    iterator = iter(events)
    close = getattr(iterator, 'close', None)
    try:
        directory = parse(iterator)
    except BaseException:
        if close is not None:
            try:
                close()
            except BaseException:
                pass
        raise
    if close is not None:
        close()
    return archive.publish(batch_id, directory)
