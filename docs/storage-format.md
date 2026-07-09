# Storage Format

The database file begins with two 4096-byte **meta slots** (8192 bytes total),
followed by append-only records starting at byte `8192`.

## Meta slots (double-buffered)

Slot 0 lives at byte `0`, slot 1 at byte `4096`. Each slot holds one committed
transaction descriptor:

```txt
bytes  0..3    magic "IMMU"
bytes  4..7    format version (uint32)
bytes  8..15   transaction id (uint64)
bytes 16..23   root node address (uint64)
bytes 24..31   live key count (uint64)
bytes 32..39   durable data length (uint64)
bytes 40..43   CRC32 of bytes 0..39
bytes 44..4095 zero padding
```

Each slot sits in its own page so a write to one can never tear the other. A
commit always writes the slot that does **not** hold the current transaction and
fsyncs it before it counts as durable, so the last good descriptor is never in
flight. On open the engine reads both slots, discards any whose CRC fails, and
adopts the survivor with the highest transaction id. If the newest commit's meta
write was torn, the previous transaction is used instead.

The `durable data length` is the file size that was committed at that
transaction. On open, any bytes beyond it are orphaned records left by a crash
mid-commit and are truncated away.

## Records

Records begin at byte `8192`. Each record is:

```txt
uint32 payload_length
uint32 payload_crc32
bytes  payload
```

The CRC lets reads detect torn writes and bit-rot instead of returning corrupt
data. Tree nodes are pickled dictionaries: a B+ tree leaf stores its sorted keys
and the addresses of their value records; a B+ tree internal node stores its
separator keys and the addresses of its child nodes. Values are codec-encoded
byte records (JSON by default).

## Durability

`connect(path, durability=...)` selects the fsync policy:

- `full` (default): appended records are fsynced before the meta block, and the
  meta block is fsynced before the commit returns. Survives process and power loss.
- `none`: no fsyncs, for throughput. A crash may lose recent commits; the
  double-buffered meta and per-record CRCs still prevent structural corruption
  when writes reach the disk, but ordering is no longer guaranteed.
