#!/usr/bin/env python3
from maelstrom import Node, Body, Request
import asyncio

node = Node()
messages = set()
cond = asyncio.Condition()

async def add_message(message_to_add: set[int]) -> None:
    # Do nothinng if it is a subset of set
    if message_to_add <= messages:
        return 
    messages.update(message_to_add)
    async with cond:
        cond.notify_all()

async def worker(neighbor):
    sent = set()
    while True:
        # assert sent <= messages
        if len(sent) == len(messages):
            async with cond:
                # Wait for the next update to our message set.
                await cond.wait_for(lambda: len(sent) != len(messages))

        to_send = messages - sent
        body = {"type": "broadcast_many", "messages": list(to_send)}
        resp = await node.rpc(neighbor, body)
        if resp["type"] == "broadcast_many_ok":
            sent.update(to_send)

@node.handler
async def broadcast(req: Request) -> Body:
    """Handle broadcast request"""
    msg = req.body["message"]
    await add_message({msg})
    return {"type": "broadcast_ok"} 

@node.handler
async def broadcast_many(req: Request) -> Body:
    """Handle broadcast_many request"""
    msg = req.body["messages"]
    await add_message(set(msg))
    return {"type": "broadcast_many_ok"} 

@node.handler
async def read(req: Request) -> Body:
    """Return all seen messages in sorted order"""
    return {"type": "read_ok", "messages": list(messages)}

@node.handler
async def topology(req: Request) -> Body:
    """Store the topology and start retry worker"""
    neighbors = req.body["topology"][node.node_id]
    # Start broadcast worker for each peer
    for n in neighbors:
        node.spawn(worker(n))
    return {"type": "topology_ok"}

if __name__ == "__main__":
    # Start worker tasks
    node.run()