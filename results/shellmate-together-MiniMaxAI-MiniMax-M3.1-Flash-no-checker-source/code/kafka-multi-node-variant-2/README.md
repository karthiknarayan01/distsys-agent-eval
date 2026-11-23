# Journals (renamed Kafka) service

A 3-node Maelstrom service that keeps append-only journals shared by all
nodes.

## Design

All durable state lives in the linearizable `lin-kv` service, so every node
observes exactly the same journal contents and bookmarks; no consensus
protocol is needed.

* Each journal is stored as a sequence of fixed-size chunks
  (`journal:<base64url(name)>:chunk:<i>`) in lin-kv.  Chunk `i` holds the
  entries for positions `[i*256, (i+1)*256)`.
* `append` reads the current chunk and compare-and-swaps an extended copy of
  it.  A failed CAS means another node appended concurrently, so the chunk is
  re-read and the CAS is retried.  The position of an entry is
  `chunkIndex*256 + len(chunk)`, which is strictly increasing and unique.
* `fetch` returns a contiguous run of `[position, entry]` pairs starting at
  the requested position (one pair per journal keeps replies small); journals
  without data at or after the requested position are omitted.
* `bookmark` advances a per-journal committed position with a CAS loop, and
  `bookmarks` reads the committed positions of the requested journals.

Client traffic is served at a steady rate so that the history produced by a
run stays small enough for Maelstrom's checker to analyse; requests are
queued, never dropped.
