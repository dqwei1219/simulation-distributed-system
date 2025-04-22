#!/usr/bin/env python3

from maelstrom import Node, Body, Request
import asyncio

node = Node()
# No need lock becasue event loop is a single thread
glob_counter = 1

@node.handler
async def generate(req: Request) -> Body:
    global glob_counter
    new_id = f"{node.node_id}_{glob_counter}"
    glob_counter += 1

    return {"type": "generate_ok", "id": new_id }

node.run()