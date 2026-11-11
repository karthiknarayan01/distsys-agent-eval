# Journals across three nodes

Write a node for a cluster of three that together keep many journals. A journal is an
append-only list of integer entries, named by a string, and each entry has a position in its
journal. Readers also leave bookmarks saying how far they have read. Clients send any of four
kinds of request to any of the three nodes, so all three must agree on every journal: the same
entry at the same position, whichever node a client asks.

`append` adds an entry to a journal and answers with its position. Positions in a journal may
start anywhere but must only increase, and no two entries in one journal share a position:

    {"type": "append", "msg_id": 1, "journal": "j7", "entry": 55}
    {"type": "appended", "in_reply_to": 1, "position": 30}

`fetch` reads several journals at once, each from a given position. For each journal it answers
`[position, entry]` pairs in position order, starting at that position or the first after it.
It may answer fewer pairs than the journal holds, and may leave out a journal with nothing at or
after the position asked for:

    {"type": "fetch", "msg_id": 2, "positions": {"j7": 30, "j9": 0}}
    {"type": "fetched", "in_reply_to": 2, "entries": {"j7": [[30, 55], [31, 8]], "j9": [[0, 4]]}}

`bookmark` records, for each journal, the position a reader has finished reading up to:

    {"type": "bookmark", "msg_id": 3, "positions": {"j7": 31}}
    {"type": "bookmarked", "in_reply_to": 3}

`bookmarks` asks for the bookmarked position of each journal named, leaving out journals with no
bookmark:

    {"type": "bookmarks", "msg_id": 4, "journals": ["j7", "j9"]}
    {"type": "bookmarks_are", "in_reply_to": 4, "positions": {"j7": 31}}

An entry whose `append` was answered must never be lost or changed: a fetch that goes past its
position must not skip it, and every answer for that position must give the same entry. Nothing
fails during the test.

The nodes can share a key/value store that Maelstrom runs as a service named `lin-kv`. It is
linearizable: a read always sees the latest write. In Go, `maelstrom.NewLinKV(node)` gives you
`Read`, `ReadInt`, `Write` and `CompareAndSwap`, each taking a `context.Context`. Nodes may also
message each other in any format you choose.
