"""Frozen declarative tree oracle: no production qualification or archive imports."""
import hashlib
import random


def expected(nodes):
    result = []
    order = 0

    def visit(children, approved):
        nonlocal order
        for node in children:
            if node['kind'] == 'group':
                visit(node['children'], approved and node['approved'])
            else:
                position = order
                order += 1
                if approved and node['approved']:
                    body = node['body']
                    result.append((position, node['id'], body,
                                   hashlib.sha256(body).hexdigest()))
    visit(nodes, True)
    return result


def events(nodes, seed=0):
    rng = random.Random(seed)

    def walk(children):
        for node in children:
            if node['kind'] == 'group':
                yield ('begin_group', node['id'])
                # The unique final verdict may precede or follow its children.
                early = rng.choice([True, False])
                if early:
                    yield ('verdict', node['approved'])
                yield from walk(node['children'])
                if not early:
                    yield ('verdict', node['approved'])
                yield ('end_group',)
            else:
                yield ('begin_record', node['id'])
                raw = node['body']
                offset = 0
                yield ('body', b'')
                while offset < len(raw):
                    width = rng.randint(1, 13)
                    yield ('body', raw[offset:offset + width])
                    offset += width
                yield ('end_record', node['approved'])
    yield from walk(nodes)
    yield ('end_batch',)


def tree(seed):
    rng = random.Random(seed)
    serial = 0

    def children(depth):
        nonlocal serial
        nodes = []
        for _ in range(rng.randint(1, 5)):
            serial += 1
            identity = str(serial)
            if depth and rng.randrange(3) == 0:
                nodes.append(dict(kind='group', id='group', approved=bool(rng.randrange(2)),
                                  children=children(depth - 1)))
            else:
                # Deliberately small alphabet and empty values create repeated bodies.
                raw = rng.choice([b'', b'same', b'\x00\xff', bytes(range(30))])
                nodes.append(dict(kind='record', id=identity, body=raw,
                                  approved=bool(rng.randrange(2))))
        return nodes
    return children(4)
