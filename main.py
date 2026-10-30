import asyncio
import json
import os
import time
import aiohttp
import websockets
from datetime import datetime, timezone

# --- TOKEN PARSING ---
# Supports BOT_TOKENS as a comma-separated string (e.g., "token1,token2")
# Or individual variables BOT_TOKEN_1, BOT_TOKEN_2, etc.
def parse_tokens():
    tokens = []
    raw = os.getenv("BOT_TOKENS", "")
    if raw:
        tokens.extend(t.strip() for t in raw.split(",") if t.strip())
    i = 1
    while True:
        t = os.getenv(f"BOT_TOKEN_{i}")
        if not t:
            break
        tokens.append(t)
        i += 1
    return tokens

TOKENS = parse_tokens()
if not TOKENS:
    raise ValueError("No tokens found. Set BOT_TOKENS or BOT_TOKEN_1, BOT_TOKEN_2, etc.")

# --- CONFIG ---
STREAM_URL = "https://www.twitch.tv/your_channel"  # Required for streaming status
STREAM_NAME = "My Stream"
STATUS = "online"  # online, idle, dnd, invisible
# How often to update the presence (to keep the connection alive). 
# Set high to save bandwidth. Discord's heartbeat is ~41.25s, this is separate.
PRESENCE_UPDATE_INTERVAL = 300  # 5 minutes

async def maintain_presence(token):
    """Handles one token: sets bio timer, connects to gateway, maintains presence."""
    start_time = int(time.time() * 1000)  # Unix timestamp in ms
    # Dynamic timestamp format: <t:UNIX:R> -> "X minutes ago"
    bio_text = f"Streaming since <t:{int(start_time/1000)}:R>"

    # --- 1. Update Profile Bio (One-time REST Call) ---
    async with aiohttp.ClientSession() as session:
        headers = {"Authorization": token, "Content-Type": "application/json"}
        try:
            async with session.patch(
                "https://discord.com/api/v9/users/@me",
                headers=headers,
                json={"bio": bio_text}
            ) as resp:
                if resp.status == 200:
                    print(f"[{token[:10]}...] Bio updated to: {bio_text}")
                else:
                    print(f"[{token[:10]}...] Bio update failed: {resp.status} - {await resp.text()}")
        except Exception as e:
            print(f"[{token[:10]}...] Bio update error: {e}")

    # --- 2. Connect to Gateway and Maintain Presence ---
    gateway_url = "wss://gateway.discord.gg/?v=9&encoding=json"
    while True:
        try:
            async with websockets.connect(gateway_url, max_size=2**23) as ws:
                hello = json.loads(await ws.recv())
                heartbeat_interval = hello['d']['heartbeat_interval'] / 1000.0

                # IDENTIFY with streaming presence
                identify_payload = {
                    "op": 2,
                    "d": {
                        "token": token,
                        "properties": {"$os": "linux", "$browser": "chrome", "$device": "desktop"},
                        "presence": {
                            "status": STATUS,
                            "afk": False,
                            "activities": [{
                                "name": STREAM_NAME,
                                "type": 1,  # 1 = STREAMING
                                "url": STREAM_URL
                            }]
                        }
                    }
                }
                await ws.send(json.dumps(identify_payload))

                # Background heartbeat task
                heartbeat_task = asyncio.create_task(send_heartbeats(ws, heartbeat_interval))

                # Send periodic presence updates to keep the streaming status fresh
                presence_update_task = asyncio.create_task(
                    send_presence_updates(ws, identify_payload, PRESENCE_UPDATE_INTERVAL)
                )

                try:
                    # Receive and ignore messages to keep the socket alive.
                    while True:
                        try:
                            await asyncio.wait_for(ws.recv(), timeout=heartbeat_interval + 5)
                        except asyncio.TimeoutError:
                            pass  # No message, but that's fine.
                except websockets.exceptions.ConnectionClosed:
                    print(f"[{token[:10]}...] Connection closed, reconnecting...")
                finally:
                    heartbeat_task.cancel()
                    presence_update_task.cancel()

        except Exception as e:
            print(f"[{token[:10]}...] Connection error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

async def send_heartbeats(ws, interval):
    """Sends HEARTBEAT (opcode 1) packets."""
    while True:
        await asyncio.sleep(interval)
        try:
            await ws.send(json.dumps({"op": 1, "d": None}))
        except Exception:
            break

async def send_presence_updates(ws, identify_payload, interval):
    """Periodically re-sends the PRESENCE_UPDATE payload to keep it fresh."""
    while True:
        await asyncio.sleep(interval)
        try:
            # Opcode 3 is PRESENCE_UPDATE
            payload = {
                "op": 3,
                "d": identify_payload["d"]["presence"]
            }
            await ws.send(json.dumps(payload))
        except Exception:
            break

async def main():
    tasks = [maintain_presence(token) for token in TOKENS]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    print(f"Starting minimal presence maintainer for {len(TOKENS)} account(s)...")
    asyncio.run(main())
