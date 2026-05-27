"""
BattleKids Game Server  —  v2
- Online mode  : Battle Royale up to 50 players
- Party mode   : private/public rooms with custom codes + optional password
- Local mode   : handled entirely client-side (no server needed)

Fixes vs v1:
  • create_party respects custom code sent by client
  • join_party checks password for private rooms
  • public-room filter uses is_public flag (not mode=="private")
  • party host-transfer when host disconnects
  • grenade broadcast in party + online
  • ping/pong keepalive so Railway doesn't close idle connections
  • lobby_state sent to joining player immediately with correct code
  • party_created includes lobby_state so waiting screen populates
"""

import asyncio, json, random, string, time, os, hashlib
import websockets
from websockets.server import serve

PORT = int(os.environ.get("PORT", 8765))

MAP_W, MAP_H = 3200, 3200

# ─────────────────────────────────────────────────────────
#  Global state
# ─────────────────────────────────────────────────────────
online_game = None   # OnlineGame | None
parties     = {}     # code -> PartyGame


# ─────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────
def gen_code(length=6):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choices(chars, k=length))


def make_player(name):
    return {
        "name":     name,
        "x":        random.randint(300, MAP_W - 300),
        "y":        random.randint(300, MAP_H - 300),
        "hp":       100,
        "max_hp":   100,
        "ammo":     30,
        "grenades": 3,
        "kills":    0,
        "alive":    True,
        "angle":    0,
    }


def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()[:16] if pw else ""


async def broadcast(clients, msg):
    """Send msg (dict) to every ws in clients, ignoring closed sockets."""
    data = json.dumps(msg)
    coros = []
    for ws in list(clients):
        try:
            coros.append(ws.send(data))
        except Exception:
            pass
    if coros:
        await asyncio.gather(*coros, return_exceptions=True)


async def send(ws, msg):
    try:
        await ws.send(json.dumps(msg))
    except Exception:
        pass


# ─────────────────────────────────────────────────────────
#  Online Battle Royale
# ─────────────────────────────────────────────────────────
class OnlineGame:
    MAX_PLAYERS = 50
    MIN_TO_START = 2        # رفعه لـ 10 في الإنتاج
    ZONE_START  = MAP_W * 0.45
    ZONE_MIN    = 150
    ZONE_SPEED  = 0.8       # pixels/sec

    def __init__(self):
        self.players    = {}    # ws -> player_data
        self.started    = False
        self.zone_r     = self.ZONE_START
        self.zone_cx    = MAP_W // 2
        self.zone_cy    = MAP_H // 2
        self.winner     = None
        self._task      = None

    # ── player management ──
    def add(self, ws, name):
        if len(self.players) >= self.MAX_PLAYERS:
            return False
        p = make_player(name)
        p["id"] = str(id(ws))
        self.players[ws] = p
        return True

    def remove(self, ws):
        p = self.players.pop(ws, None)
        if p:
            p["alive"] = False

    def alive_count(self):
        return sum(1 for p in self.players.values() if p["alive"])

    # ── broadcast helpers ──
    def _all(self):
        return set(self.players.keys())

    def state_msg(self):
        return {
            "type":    "state",
            "players": {str(id(ws)): p for ws, p in self.players.items()},
            "zone_r":  self.zone_r,
            "zone_cx": self.zone_cx,
            "zone_cy": self.zone_cy,
            "alive":   self.alive_count(),
            "started": self.started,
        }

    # ── start / loop ──
    async def start(self):
        self.started = True
        await broadcast(self._all(), {"type": "game_start", "mode": "online"})
        self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        while self.players:
            await asyncio.sleep(0.05)   # 20 Hz

            # shrink zone
            if self.zone_r > self.ZONE_MIN:
                self.zone_r = max(self.ZONE_MIN,
                                  self.zone_r - self.ZONE_SPEED * 0.05)

            # zone damage
            for ws, p in list(self.players.items()):
                if not p["alive"]:
                    continue
                dx = p["x"] - self.zone_cx
                dy = p["y"] - self.zone_cy
                if (dx*dx + dy*dy) ** 0.5 > self.zone_r:
                    p["hp"] -= 1
                    if p["hp"] <= 0:
                        p["hp"]    = 0
                        p["alive"] = False
                        await send(ws, {"type": "eliminated", "reason": "zone"})

            # winner check
            alive = [(ws, p) for ws, p in self.players.items() if p["alive"]]
            if len(alive) == 1:
                self.winner = alive[0][1]["name"]
                await broadcast(self._all(),
                                {"type": "game_over", "winner": self.winner})
                return
            if len(alive) == 0:
                await broadcast(self._all(),
                                {"type": "game_over", "winner": None})
                return

            await broadcast(self._all(), self.state_msg())


# ─────────────────────────────────────────────────────────
#  Party Room
# ─────────────────────────────────────────────────────────
class PartyGame:
    def __init__(self, code, host_ws, host_name,
                 mode="coop", is_public=True, password=""):
        self.code      = code
        self.host      = host_ws
        self.mode      = mode
        self.is_public = is_public
        self.pw_hash   = hash_pw(password)   # "" means no password
        self.players   = {host_ws: make_player(host_name)}
        self.started   = False
        self._task     = None

    # ── player management ──
    def add(self, ws, name):
        p = make_player(name)
        p["id"] = str(id(ws))
        self.players[ws] = p

    def remove(self, ws):
        self.players.pop(ws, None)
        # transfer host if needed
        if ws == self.host and self.players:
            self.host = next(iter(self.players))

    def check_password(self, pw: str) -> bool:
        if not self.pw_hash:          # no password set
            return True
        return hash_pw(pw) == self.pw_hash

    def _all(self):
        return set(self.players.keys())

    # ── lobby state ──
    def lobby_msg(self):
        host_name = ""
        if self.host in self.players:
            host_name = self.players[self.host]["name"]
        return {
            "type":      "lobby_state",
            "code":      self.code,
            "players":   [p["name"] for p in self.players.values()],
            "host":      host_name,
            "started":   self.started,
            "mode":      self.mode,
            "is_public": self.is_public,
        }

    # ── start / loop ──
    async def start(self):
        self.started = True
        await broadcast(self._all(), {
            "type":    "game_start",
            "mode":    "party",
            "players": {str(id(ws)): p for ws, p in self.players.items()},
        })
        self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        """Party state broadcast loop — also handles winner detection for PVP."""
        while self.players:
            await asyncio.sleep(0.05)

            # ── PVP winner check ──
            if self.mode == "pvp":
                alive = [(ws, p) for ws, p in self.players.items() if p.get("alive", True)]
                if len(alive) == 1:
                    winner_name = alive[0][1]["name"]
                    await broadcast(self._all(), {"type": "game_over", "winner": winner_name})
                    return
                elif len(alive) == 0:
                    await broadcast(self._all(), {"type": "game_over", "winner": None})
                    return

            await broadcast(self._all(), {
                "type":    "state",
                "players": {str(id(ws)): p
                            for ws, p in self.players.items()},
            })


# ─────────────────────────────────────────────────────────
#  WebSocket handler
# ─────────────────────────────────────────────────────────
async def handler(ws):
    global online_game

    player_name  = None
    current_game = None   # "online" | party_code
    party_code   = None

    try:
        async for raw in ws:
            # ── parse ──
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            t = msg.get("type", "")

            # ════════════════════════════════════════════
            #  PING  (keepalive)
            # ════════════════════════════════════════════
            if t == "ping":
                await send(ws, {"type": "pong"})

            # ════════════════════════════════════════════
            #  LIST PUBLIC PARTIES
            # ════════════════════════════════════════════
            elif t == "list_parties":
                public = [
                    {"code": c, "host": g.players[g.host]["name"] if g.host in g.players else "?",
                     "players": len(g.players), "mode": g.mode}
                    for c, g in parties.items()
                    if not g.started and g.is_public
                ]
                await send(ws, {"type": "parties_list", "parties": public})

            # ════════════════════════════════════════════
            #  DELETE PARTY  (host only)
            # ════════════════════════════════════════════
            elif t == "delete_party":
                if party_code and party_code in parties:
                    game = parties[party_code]
                    if ws == game.host:
                        await broadcast(game._all(), {"type": "party_deleted", "code": party_code})
                        parties.pop(party_code, None)
                        party_code   = None
                        current_game = None

            # ════════════════════════════════════════════
            #  ONLINE  —  Battle Royale
            # ════════════════════════════════════════════
            elif t == "join_online":
                player_name = msg.get("name", "Player")[:20]

                # start fresh game if needed
                if online_game is None or \
                   len(online_game.players) >= OnlineGame.MAX_PLAYERS or \
                   online_game.started:
                    online_game = OnlineGame()

                online_game.add(ws, player_name)
                current_game = "online"

                await send(ws, {
                    "type":      "joined_online",
                    "player_id": str(id(ws)),
                    "players":   len(online_game.players),
                })

                await broadcast(online_game._all(), {
                    "type":    "player_joined",
                    "name":    player_name,
                    "players": len(online_game.players),
                })

                if len(online_game.players) >= OnlineGame.MIN_TO_START \
                        and not online_game.started:
                    await online_game.start()

            # ════════════════════════════════════════════
            #  PARTY  —  Create room
            # ════════════════════════════════════════════
            elif t == "create_party":
                player_name = msg.get("name", "Host")[:20]
                mode        = msg.get("mode",   "coop")
                is_public   = bool(msg.get("public", True))
                password    = msg.get("password", "")
                custom_code = msg.get("code",  "").strip().upper()

                # use custom code if valid and not taken, else generate one
                if custom_code and len(custom_code) <= 12 \
                        and custom_code not in parties:
                    code = custom_code
                else:
                    code = gen_code()
                    while code in parties:
                        code = gen_code()

                game = PartyGame(code, ws, player_name,
                                 mode=mode,
                                 is_public=is_public,
                                 password=password)
                parties[code] = game
                party_code    = code
                current_game  = code

                # send party_created first (client stores NET.party_code)
                await send(ws, {
                    "type":      "party_created",
                    "code":      code,
                    "public":    is_public,
                    "mode":      mode,
                    "player_id": str(id(ws)),
                })
                # then send lobby state so waiting screen shows players
                await send(ws, game.lobby_msg())

            # ════════════════════════════════════════════
            #  PARTY  —  Join room
            # ════════════════════════════════════════════
            elif t == "join_party":
                player_name = msg.get("name", "Player")[:20]
                code        = msg.get("code", "").strip().upper()
                password    = msg.get("password", "")

                # no code → find first open public room
                if not code:
                    public = [(c, g) for c, g in parties.items()
                              if not g.started and g.is_public]
                    if public:
                        code, game = public[0]
                    else:
                        await send(ws, {"type": "error",
                                        "msg":  "مفيش غرف عامة متاحة. اعمل واحدة!"})
                        continue
                elif code not in parties:
                    await send(ws, {"type": "error",
                                    "msg":  "كود الغرفة غلط"})
                    continue
                else:
                    game = parties[code]

                if game.started:
                    await send(ws, {"type": "error",
                                    "msg":  "اللعبة بدأت بالفعل"})
                    continue

                # password check
                if not game.check_password(password):
                    await send(ws, {"type": "error",
                                    "msg":  "كلمة السر غلطانة"})
                    continue

                game.add(ws, player_name)
                party_code   = code
                current_game = code

                # tell joining player their id + code
                await send(ws, {
                    "type":      "joined_party",
                    "code":      code,
                    "player_id": str(id(ws)),
                })

                # update everyone in lobby
                await broadcast(game._all(), game.lobby_msg())

            # ════════════════════════════════════════════
            #  PARTY  —  Start (host only)
            # ════════════════════════════════════════════
            elif t == "start_party":
                if party_code and party_code in parties:
                    game = parties[party_code]
                    if ws == game.host and not game.started:
                        await game.start()

            # ════════════════════════════════════════════
            #  PARTY  —  Change mode (host only)
            # ════════════════════════════════════════════
            elif t == "set_mode":
                if party_code and party_code in parties:
                    game = parties[party_code]
                    if ws == game.host:
                        game.mode = msg.get("mode", game.mode)
                        await broadcast(game._all(), game.lobby_msg())

            # ════════════════════════════════════════════
            #  PLAYER UPDATE  (position / hp / alive)
            # ════════════════════════════════════════════
            elif t == "player_update":
                p_data = msg.get("player", {})
                update = {
                    "x":     p_data.get("x",     0),
                    "y":     p_data.get("y",     0),
                    "angle": p_data.get("angle", 0),
                    "hp":    p_data.get("hp",  100),
                    "alive": p_data.get("alive", True),
                }
                if current_game == "online" and online_game \
                        and ws in online_game.players:
                    online_game.players[ws].update(update)

                elif current_game and current_game in parties:
                    game = parties[current_game]
                    if ws in game.players:
                        game.players[ws].update(update)

            # ════════════════════════════════════════════
            #  BULLET
            # ════════════════════════════════════════════
            elif t == "bullet":
                b = msg.get("bullet", {})
                b["owner"] = str(id(ws))
                out = {"type": "bullet", "bullet": b}

                if current_game == "online" and online_game:
                    await broadcast(online_game._all() - {ws}, out)
                elif current_game and current_game in parties:
                    await broadcast(parties[current_game]._all() - {ws}, out)

            # ════════════════════════════════════════════
            #  GRENADE
            # ════════════════════════════════════════════
            elif t == "grenade":
                g = msg.get("grenade", {})
                g["owner"] = str(id(ws))
                out = {"type": "grenade", "grenade": g}

                if current_game == "online" and online_game:
                    await broadcast(online_game._all() - {ws}, out)
                elif current_game and current_game in parties:
                    await broadcast(parties[current_game]._all() - {ws}, out)

            # ════════════════════════════════════════════
            #  HIT  (client-side hit detection)
            # ════════════════════════════════════════════
            elif t == "hit":
                target_id = msg.get("target")
                damage    = max(0, min(int(msg.get("damage", 0)), 100))

                pool = {}
                if current_game == "online" and online_game:
                    pool = online_game.players
                elif current_game and current_game in parties:
                    pool = parties[current_game].players

                for tw, tp in pool.items():
                    if str(id(tw)) == target_id and tp["alive"]:
                        tp["hp"] = max(0, tp["hp"] - damage)
                        if tp["hp"] <= 0:
                            tp["alive"] = False
                            # give kill credit to shooter
                            if ws in pool:
                                pool[ws]["kills"] = pool[ws].get("kills", 0) + 1
                            await send(tw, {"type": "eliminated",
                                            "reason": "shot",
                                            "by": player_name})
                        break

            # ════════════════════════════════════════════
            #  CHAT
            # ════════════════════════════════════════════
            elif t == "chat":
                text = str(msg.get("text", ""))[:100]
                out  = {"type": "chat",
                        "name": player_name or "?",
                        "text": text}
                if current_game == "online" and online_game:
                    await broadcast(online_game._all(), out)
                elif current_game and current_game in parties:
                    await broadcast(parties[current_game]._all(), out)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        # ── clean up on disconnect ──
        if current_game == "online" and online_game:
            online_game.remove(ws)

        elif current_game and current_game in parties:
            game = parties[current_game]
            game.remove(ws)

            if not game.players:
                # last player left → delete room
                parties.pop(current_game, None)
            else:
                # notify remaining players
                await broadcast(game._all(), game.lobby_msg())


# ─────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────
async def main():
    print(f"BattleKids Server v2  —  port {PORT}")
    async with serve(handler, "0.0.0.0", PORT,
                     ping_interval=20, ping_timeout=60):
        await asyncio.Future()   # run forever


if __name__ == "__main__":
    asyncio.run(main())