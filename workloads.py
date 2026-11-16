"""Deterministic finite catalogue loads, independent expected output specification."""
import hashlib

RECORD_BYTES = 65536
CASES = ('late-veto', 'interleaved')


def record_body(number):
    pattern = bytes((number + offset) % 256 for offset in range(256))
    return pattern * (RECORD_BYTES // 256)


def count_records(mib):
    if type(mib) is not int or mib <= 0:
        raise ValueError('mib must be a positive integer')
    return mib * 1024 * 1024 // RECORD_BYTES


def record_events(number):
    yield ('begin_record', 'r' + str(number))
    raw = record_body(number)
    offset = step = 0
    widths = (4093, 8191, 6151)
    while offset < len(raw):
        width = widths[step % len(widths)]
        yield ('body', raw[offset:offset + width])
        offset += width
        step += 1
    yield ('end_record', number % 11 != 0)


def events(case, mib):
    count = count_records(mib)
    if case not in CASES:
        raise ValueError('unknown workload')
    yield ('begin_group', 'root')
    if case == 'late-veto':
        for number in range(count):
            yield from record_events(number)
        yield ('verdict', False)
    else:
        for block, start in enumerate(range(0, count, 8)):
            yield from record_events(start)
            depth = block % 7 + 1
            for level in range(depth):
                yield ('begin_group', 'g-' + str(block) + '-' + str(level))
            for number in range(start + 1, min(start + 8, count)):
                yield from record_events(number)
            for level in reversed(range(depth)):
                yield ('verdict', not (level == depth - 1 and block % 4 == 1))
                yield ('end_group',)
        yield ('verdict', True)
    yield ('end_group',)
    yield ('end_batch',)


def expected(case, mib):
    count = count_records(mib)
    if case not in CASES:
        raise ValueError('unknown workload')
    if case == 'late-veto':
        return
    for number in range(count):
        block, local = divmod(number, 8)
        eligible = number % 11 != 0 and (local == 0 or block % 4 != 1)
        if eligible:
            raw = record_body(number)
            yield number, 'r' + str(number), raw, hashlib.sha256(raw).hexdigest()


def verify(archive, receipt, case, mib):
    expected_count = expected_bytes = 0
    with archive.read_records(receipt.batch_id) as rows:
        for position, identity, body, digest in expected(case, mib):
            row = next(rows)
            assert (row.sequence, row.source_order, row.id, row.body, row.sha256) == (
                expected_count, position, identity, body, digest)
            expected_count += 1
            expected_bytes += len(body)
        sentinel = object()
        assert next(rows, sentinel) is sentinel, 'unexpected extra record'
    assert receipt.record_count == expected_count
    assert receipt.total_bytes == expected_bytes
    assert archive.receipt(receipt.batch_id) == receipt
