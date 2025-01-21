# Storage Format

The database file starts with a 4096-byte superblock.

- Bytes `0..7`: unsigned 64-bit big-endian root node address.
- Bytes `8..4095`: reserved for future metadata.
- Records begin at byte `4096`.

Each record is stored as:

```txt
uint32 payload_length
bytes  payload
```

Tree nodes are pickled dictionaries containing child addresses, key, value address, and subtree length. Values are stored as codec-encoded byte records. The default codec is JSON.
