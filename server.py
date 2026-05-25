"""
BattleKids Game Server
- Online mode: Battle Royale 50 players, no rooms
- Party mode: private rooms with codes
- Local mode: handled client-side
"""
import asyncio, json, random, string, time, os
import websockets
from websockets.server import serve

PORT = int(os.environ.get("PORT", 8765))

# ── Game state ──────────────────────────────────────────
MAP_W, MAP_H = 3200, 3200

online_players  = {}   # ws -> player_data
online_game     = None # current online game state

parties         = {}   # code -> {host, players, started, game}

# ── Helpers ─────────────────────────────────────────────
def gen_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

def make_player(name, x=None, y=None):
    return {
        "name":    name,
        "x":       x or random.randint(200, MAP_W-200),
        "y":       y or random.randint(200, MAP_H-200),
        "hp":      100,
        "max_hp":  100,
        "ammo":    30,
        "grenades":3,
        "kills":   0,
        "alive":   True,
        "angle":   0,
    }

def broadcast_json(clients, msg):
    """Broadcast to a set of websocket clients"""
    data = json.dumps(msg)
    return asyncio.gather(*[ws.send(data) for ws in clients if not ws.closed], return_exceptions=True)

# ── Online Battle Royale ─────────────────────────────────
class OnlineGame:
    MAX_PLAYERS = 50
    ZONE_START  = MAP_W * 0.45
    ZONE_MIN    = 150
    ZONE_SPEED  = 0.8  # pixels per second shrink

    def __init__(self):
        self.players   = {}   # ws -> player_data
        self.started   = False
        self.start_time= None
        self.zone_r    = self.ZONE_START
        self.zone_cx   = MAP_W // 2
        self.zone_cy   = MAP_H // 2
        self.winner    = None
        self.task      = None

    def add_player(self, ws, name):
        if len(self.players) >= self.MAX_PLAYERS:
            return False
        p = make_player(name)
        p["id"] = id(ws)
        self.players[ws] = p
        return True

    def remove_player(self, ws):
        if ws in self.players:
            self.players[ws]["alive"] = False
            del self.players[ws]

    def alive_count(self):
        return sum(1 for p in self.players.values() if p["alive"])

    def get_state(self):
        return {
            "type":     "state",
            "players":  {str(id(ws)): p for ws, p in self.players.items()},
            "zone_r":   self.zone_r,
            "zone_cx":  self.zone_cx,
            "zone_cy":  self.zone_cy,
            "alive":    self.alive_count(),
            "started":  self.started,
        }

    async def start(self):
        self.started    = True
        self.start_time = time.time()
        await broadcast_json(set(self.players.keys()), {"type": "game_start", "mode": "online"})
        self.task = asyncio.create_task(self.game_loop())

    async def game_loop(self):
        while True:
            await asyncio.sleep(0.05)  # 20 ticks/sec
            if not self.players:
                break

            # shrink zone
            if self.started and self.zone_r > self.ZONE_MIN:
                self.zone_r = max(self.ZONE_MIN, self.zone_r - self.ZONE_SPEED * 0.05)

            # zone damage
            for ws, p in list(self.players.items()):
                if not p["alive"]: continue
                dx = p["x"] - self.zone_cx
                dy = p["y"] - self.zone_cy
                dist = (dx*dx + dy*dy) ** 0.5
                if dist > self.zone_r:
                    p["hp"] -= 1
                    if p["hp"] <= 0:
                        p["alive"] = False
                        p["hp"] = 0
                        await ws.send(json.dumps({"type": "eliminated", "reason": "zone"}))

            # check winner
            alive = [(ws, p) for ws, p in self.players.items() if p["alive"]]
            if len(alive) == 1:
                winner_ws, winner_p = alive[0]
                self.winner = winner_p["name"]
                await broadcast_json(set(self.players.keys()), {
                    "type": "game_over",
                    "winner": self.winner
                })
                break
            elif len(alive) == 0:
                await broadcast_json(set(self.players.keys()), {"type": "game_over", "winner": None})
                break

            # broadcast state
            await broadcast_json(set(self.players.keys()), self.get_state())


# ── Party Game ───────────────────────────────────────────
class PartyGame:
    def __init__(self, code, host_name, host_ws, mode="coop"):
        self.code     = code
        self.host     = host_ws
        self.mode     = mode
        self.players  = {host_ws: make_player(host_name)}
        self.started  = False
        self.task     = None

    def add_player(self, ws, name):
        p = make_player(name)
        p["id"] = id(ws)
        self.players[ws] = p

    def remove_player(self, ws):
        if ws in self.players:
            del self.players[ws]

    def get_lobby_state(self):
        return {
            "type":    "lobby_state",
            "code":    self.code,
            "players": [p["name"] for p in self.players.values()],
            "host":    self.players.get(self.host, {}).get("name", ""),
            "started": self.started,
            "mode":    self.mode,
        }

    async def start(self):
        self.started = True
        await broadcast_json(set(self.players.keys()), {
            "type": "game_start",
            "mode": "party",
            "players": {str(id(ws)): p for ws, p in self.players.items()}
        })
        self.task = asyncio.create_task(self.game_loop())

    async def game_loop(self):
        while True:
            await asyncio.sleep(0.05)
            if not self.players:
                break
            state = {
                "type":    "state",
                "players": {str(id(ws)): p for ws, p in self.players.items()},
            }
            await broadcast_json(set(self.players.keys()), state)


# ── WebSocket Handler ────────────────────────────────────
async def handler(ws):
    global online_game

    player_name  = None
    current_game = None  # "online" | party_code
    party_code   = None

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except:
                continue

            t = msg.get("type")

            # ── Join online Battle Royale ──
            if t == "join_online":
                player_name = msg.get("name", "Player")

                if online_game is None or len(online_game.players) >= OnlineGame.MAX_PLAYERS:
                    online_game = OnlineGame()

                online_game.add_player(ws, player_name)
                current_game = "online"

                await ws.send(json.dumps({
                    "type":      "joined_online",
                    "player_id": str(id(ws)),
                    "players":   len(online_game.players),
                }))

                await broadcast_json(set(online_game.players.keys()), {
                    "type":    "player_joined",
                    "name":    player_name,
                    "players": len(online_game.players),
                })

                # auto-start when 2+ players (for testing), change to 10+ for production
                if len(online_game.players) >= 2 and not online_game.started:
                    await online_game.start()

            # ── Create party ──
            elif t == "create_party":
                player_name = msg.get("name", "Host")
                mode        = msg.get("mode", "coop")
                code        = gen_code()
                while code in parties:
                    code = gen_code()

                game = PartyGame(code, player_name, ws, mode)
                parties[code] = game
                party_code    = code
                current_game  = code

                await ws.send(json.dumps({
                    "type": "party_created",
                    "code": code,
                }))

            # ── Join party ──
            elif t == "join_party":
                player_name = msg.get("name", "Player")
                code        = msg.get("code", "").upper()

                if code not in parties:
                    await ws.send(json.dumps({"type": "error", "msg": "كود الغرفة غلط"}))
                    continue

                game = parties[code]
                if game.started:
                    await ws.send(json.dumps({"type": "error", "msg": "اللعبة بدأت بالفعل"}))
                    continue

                game.add_player(ws, player_name)
                party_code   = code
                current_game = code

                await broadcast_json(set(game.players.keys()), game.get_lobby_state())

            # ── Start party (host only) ──
            elif t == "start_party":
                code = party_code
                if code and code in parties:
                    game = parties[code]
                    if ws == game.host:
                        await game.start()

            # ── Player move/shoot update ──
            elif t == "player_update":
                p_data = msg.get("player", {})
                if current_game == "online" and online_game and ws in online_game.players:
                    online_game.players[ws].update({
                        "x":     p_data.get("x", 0),
                        "y":     p_data.get("y", 0),
                        "angle": p_data.get("angle", 0),
                        "hp":    p_data.get("hp", 100),
                        "alive": p_data.get("alive", True),
                    })
                elif current_game and current_game in parties:
                    game = parties[current_game]
                    if ws in game.players:
                        game.players[ws].update({
                            "x":     p_data.get("x", 0),
                            "y":     p_data.get("y", 0),
                            "angle": p_data.get("angle", 0),
                            "hp":    p_data.get("hp", 100),
                            "alive": p_data.get("alive", True),
                        })

            # ── Bullet fired ──
            elif t == "bullet":
                b = msg.get("bullet", {})
                b["owner"] = str(id(ws))
                if current_game == "online" and online_game:
                    await broadcast_json(
                        set(online_game.players.keys()) - {ws},
                        {"type": "bullet", "bullet": b}
                    )
                elif current_game and current_game in parties:
                    game = parties[current_game]
                    await broadcast_json(
                        set(game.players.keys()) - {ws},
                        {"type": "bullet", "bullet": b}
                    )

            # ── Hit registered ──
            elif t == "hit":
                target_id = msg.get("target")
                damage    = msg.get("damage", 0)
                if current_game == "online" and online_game:
                    for tw, tp in online_game.players.items():
                        if str(id(tw)) == target_id:
                            tp["hp"] = max(0, tp["hp"] - damage)
                            if tp["hp"] <= 0:
                                tp["alive"] = False
                                await tw.send(json.dumps({"type": "eliminated", "reason": "shot"}))
                            break

            # ── Chat ──
            elif t == "chat":
                text = msg.get("text", "")[:100]
                out  = {"type": "chat", "name": player_name, "text": text}
                if current_game == "online" and online_game:
                    await broadcast_json(set(online_game.players.keys()), out)
                elif current_game and current_game in parties:
                    await broadcast_json(set(parties[current_game].players.keys()), out)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        # cleanup
        if current_game == "online" and online_game:
            online_game.remove_player(ws)
        elif current_game and current_game in parties:
            game = parties[current_game]
            game.remove_player(ws)
            if not game.players:
                del parties[current_game]
            else:
                await broadcast_json(set(game.players.keys()), game.get_lobby_state())


async def main():
    print(f"BattleKids Server running on port {PORT}")
    async with serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
