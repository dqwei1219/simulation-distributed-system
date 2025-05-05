#!/usr/bin/env python3
from maelstrom import Node, Body, Request

node = Node()
localStore = 0

async def readInt(neighbor: int) -> int:
    global localStore
    if neighbor == node.node_id:
        return localStore

    body = {"type": "read_local"}
    while True:
        resp = await node.rpc(neighbor, body)
        if resp["type"] == "read_local_ok":
            return int(resp["value"])

@node.handler
async def read_local(req: Request) -> Body:
    global localStore
    return {"type": "read_local_ok", "value": localStore}

@node.handler
async def read(req: Request) -> Body:
    sum_of_all_nodes = 0
    for n in node.node_ids:
        sum_of_all_nodes += await readInt(n)
    return {"type": "read_ok", "value": sum_of_all_nodes}

@node.handler
async def add(req: Request) -> Body:
    global localStore
    localStore += int(req.body["delta"])
    return {"type": "add_ok"}

if __name__ == "__main__":
    # Start worker tasks
    node.run()