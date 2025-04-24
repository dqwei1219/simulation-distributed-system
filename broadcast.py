#!/usr/bin/env python3
from maelstrom import Node, Body, Request
from collections import defaultdict, deque
import asyncio

node = Node()
seen = set()
messages_lock = asyncio.Lock()
pending_messages = defaultdict(deque) 
peers = set()

async def worker(peer):
    while True:
        messages = pending_messages[peer]
        while messages:
            msg = messages.popleft()
            try:
                resp = await node.rpc(peer, {"type": "broadcast", "message": msg})
                if resp["type"] != "broadcast_ok":
                    messages.appendleft(msg)  # Re-insert at front to retry immediately
                    break
            except Exception:
                messages.appendleft(msg)  # Re-insert at front to retry immediately
                break
        await asyncio.sleep(0.1) 

@node.handler
async def broadcast(req: Request) -> Body:
    message = req.body["message"]
    if message in seen:
        return {"type": "broadcast_ok"}

    async with messages_lock:
        seen.add(message)

    # Queue messages for peers
    for peer in peers: 
        if peer != req.src and peer != node.node_id:
            pending_messages[peer].append(message)
    return {"type": "broadcast_ok"} 

@node.handler
async def read(req: Request) -> Body:
    """Return all seen messages in sorted order"""
    async with messages_lock:
        temp = sorted(seen)
    return {"type": "read_ok", "messages": temp}

@node.handler
async def topology(req: Request) -> Body:
    """Store the topology and start retry worker"""
    global peers
    peers = set(req.body["topology"].get(node.node_id, []))
    # Start broadcast worker for each peer
    for p in peers:
        node.spawn(worker(p))
    return {"type": "topology_ok"}

if __name__ == "__main__":
    # Start worker tasks
    node.run()