# Ticket numbers

Write a node for a box office spread over five machines. Each machine issues tickets, and no two
tickets may ever carry the same number, whichever machines issued them.

A client asks any node to issue a ticket:

    {"type": "issue", "msg_id": 7}

and the node answers with the ticket it issued:

    {"type": "issued", "in_reply_to": 7, "ticket": "n3-204"}

A ticket can be any JSON value: a string, a number, an array. Clients ask for about a thousand
tickets a second, spread over all five nodes, and from time to time the network splits the
nodes into groups that can't reach each other. Every request must still be answered at once,
even by a node that is cut off from all the others, and no ticket may repeat for the whole test.
