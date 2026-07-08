# Storage Format

The database file starts with a 4096-byte superblock.

- Bytes `0..7`: unsigned 64-bit big-endian root node address.
- Bytes `8..15`: unsigned 64-bit big-endian live key count (used for O(1) `len()`).
- Bytes `16..4095`: reserved for future metadata.
- Records begin at byte `4096`.

The root address and key count are written together as a single 16-byte record during commit, so a reader never observes a new root paired with a stale count.

Each record is stored as:

```txt
uint32 payload_length
bytes  payload
```

Tree nodes are pickled dictionaries. For the default B+ tree engine, a leaf node stores its sorted keys and the addresses of their value records; an internal node stores its separator keys and the addresses of its child nodes. (The reference binary tree engine instead stores child addresses, a key, a value address, and a subtree length.) Values are stored as codec-encoded byte records. The default codec is JSON.
