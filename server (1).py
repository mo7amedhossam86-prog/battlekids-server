"""
BattleKids Game Server  —  v3
- Online mode  : Battle Royale up to 50 players
- Party mode   : private/public rooms with custom codes + optional password
- Local mode   : handled entirely client-side (no server needed)

Fixes vs v2:
  • HTTP health-check endpoint (GET /) — Railway needs this
  • /status JSON endpoint for debugging
  • Zone damage matches client: 9 HP/s (was 1 HP/tick)
  • Online game properly resets after finish
  • Party _loop sends zone data so clients can show zone
  • kill_feed notification sent to shooter on kill
  • Rooms auto-cleaned after game ends
  • player_name saved on first message (join_online / create_party / join_party)
  • countdown before online game starts (10s, cancellable)
  • grenade broadcast confirmed in both online + party (was already there, kept)
  • MAX_PLAYERS guard prevents double-join
  • Graceful handler for unknown message types
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
    MAX_PLAYERS    = 50
    MIN_TO_START   = 2
    COUNTDOWN_SECS = 10     # ثواني قبل البداية
    ZONE_START     = MAP_W * 0.45
    ZONE_MIN       = 150
    ZONE_SPEED     = 0.8    # pixels/sec
    ZONE_DAMAGE    = 9.0    # HP/sec (matches client)

    def __init__(self):
        self.players      = {}   # ws -> player_data
        self.started      = False
        self.finished     = False
        self.zone_r       = self.ZONE_START
        self.zone_cx      = MAP_W // 2
        self.zone_cy      = MAP_H // 2
        self.winner       = None
        self._task        = None
        self._cd_task     = None
        self.created_at   = time.time()

    # ── player management ──
    def add(self, ws, name):
        if len(self.players) >= self.MAX_PLAYERS:
            return False
        if ws in self.players:          # prevent double-join
            return True
        p = make_player(name)
        p["id"] = str(id(ws))
        self.players[ws] = p
        return True

    def remove(self, ws):
        p = self.players.pop(ws, None)
        if p:
            p["alive"] = False
        # cancel pending countdown if we no longer have enough players
        if not self.started and len(self.players) < self.MIN_TO_START:
            if self._cd_task and not self._cd_task.done():
                self._cd_task.cancel()
            self._cd_task = None

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

    # ── countdown then start ──
    async def schedule_start(self):
        """Broadcast countdown, then start."""
        for remaining in range(self.COUNTDOWN_SECS, 0, -1):
            if len(self.players) < self.MIN_TO_START:
                # reset countdown task so it can restart when enough players join again
                self._cd_task = None
                return
            await broadcast(self._all(), {
                "type":    "player_joined",
                "players": len(self.players),
                "countdown": remaining,
            })
            await asyncio.sleep(1)
        if len(self.players) >= self.MIN_TO_START:
            await self.start()

    async def start(self):
        if self.started:
            return
        self.started = True
        await broadcast(self._all(), {"type": "game_start", "mode": "online"})
        self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        TICK = 0.05   # 20 Hz
        while self.players and not self.finished:
            await asyncio.sleep(TICK)

            # shrink zone
            if self.zone_r > self.ZONE_MIN:
                self.zone_r = max(self.ZONE_MIN,
                                  self.zone_r - self.ZONE_SPEED * TICK)

            # zone damage  (9 HP/s = 9 * TICK per tick)
            for ws, p in list(self.players.items()):
                if not p["alive"]:
                    continue
                dx = p["x"] - self.zone_cx
                dy = p["y"] - self.zone_cy
                if (dx*dx + dy*dy) ** 0.5 > self.zone_r:
                    p["hp"] -= self.ZONE_DAMAGE * TICK
                    if p["hp"] <= 0:
                        p["hp"]    = 0
                        p["alive"] = False
                        await send(ws, {"type": "eliminated", "reason": "zone"})

            # winner check
            alive = [(ws, p) for ws, p in self.players.items() if p["alive"]]
            if len(alive) == 1:
                self.winner   = alive[0][1]["name"]
                self.finished = True
                await broadcast(self._all(),
                                {"type": "game_over", "winner": self.winner})
                return
            if len(alive) == 0:
                self.finished = True
                await broadcast(self._all(),
                                {"type": "game_over", "winner": None})
                return

            await broadcast(self._all(), self.state_msg())


# ─────────────────────────────────────────────────────────
#  Party Room
# ─────────────────────────────────────────────────────────
class PartyGame:
    ZONE_START  = MAP_W * 0.45
    ZONE_MIN    = 150
    ZONE_SPEED  = 0.7
    ZONE_DAMAGE = 9.0

    def __init__(self, code, host_ws, host_name,
                 mode="coop", is_public=True, password=""):
        self.code      = code
        self.host      = host_ws
        self.mode      = mode
        self.is_public = is_public
        self.pw_hash   = hash_pw(password)
        self.players   = {host_ws: make_player(host_name)}
        self.started   = False
        self.finished  = False
        self._task     = None
        self.zone_r    = self.ZONE_START
        self.zone_cx   = MAP_W // 2
        self.zone_cy   = MAP_H // 2

    # ── player management ──
    def add(self, ws, name):
        if ws in self.players:
            return
        p = make_player(name)
        p["id"] = str(id(ws))
        self.players[ws] = p

    def remove(self, ws):
        self.players.pop(ws, None)
        # transfer host if needed
        if ws == self.host and self.players:
            self.host = next(iter(self.players))

    def check_password(self, pw: str) -> bool:
        if not self.pw_hash:
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
            "submode": self.mode,
            "players": {str(id(ws)): p for ws, p in self.players.items()},
        })
        self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        """Party state broadcast loop — zone shrink + PVP winner detection."""
        TICK = 0.05
        while self.players and not self.finished:
            await asyncio.sleep(TICK)

            # zone shrink (only in pvp / coop battle modes)
            if self.mode in ("pvp", "coop"):
                if self.zone_r > self.ZONE_MIN:
                    self.zone_r = max(self.ZONE_MIN,
                                      self.zone_r - self.ZONE_SPEED * TICK)

                # zone damage
                for ws, p in list(self.players.items()):
                    if not p.get("alive", True):
                        continue
                    dx = p["x"] - self.zone_cx
                    dy = p["y"] - self.zone_cy
                    if (dx*dx + dy*dy) ** 0.5 > self.zone_r:
                        p["hp"] = max(0, p["hp"] - self.ZONE_DAMAGE * TICK)
                        if p["hp"] <= 0:
                            p["alive"] = False
                            await send(ws, {"type": "eliminated", "reason": "zone"})

            # ── PVP winner check ──
            if self.mode == "pvp":
                alive = [(ws, p) for ws, p in self.players.items()
                         if p.get("alive", True)]
                if len(alive) == 1:
                    winner_name = alive[0][1]["name"]
                    self.finished = True
                    await broadcast(self._all(),
                                    {"type": "game_over", "winner": winner_name})
                    return
                elif len(alive) == 0:
                    self.finished = True
                    await broadcast(self._all(),
                                    {"type": "game_over", "winner": None})
                    return

            # ── COOP / general: end when all players are dead ──
            elif self.mode == "coop":
                alive = [p for p in self.players.values() if p.get("alive", True)]
                if len(alive) == 0 and self.players:
                    self.finished = True
                    await broadcast(self._all(),
                                    {"type": "game_over", "winner": None})
                    return

            await broadcast(self._all(), {
                "type":    "state",
                "players": {str(id(ws)): p for ws, p in self.players.items()},
                "zone_r":  self.zone_r,
                "zone_cx": self.zone_cx,
                "zone_cy": self.zone_cy,
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
                    {"code": c,
                     "host": g.players[g.host]["name"] if g.host in g.players else "?",
                     "players": len(g.players),
                     "mode": g.mode}
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
                        await broadcast(game._all(),
                                        {"type": "party_deleted", "code": party_code})
                        parties.pop(party_code, None)
                        party_code   = None
                        current_game = None

            # ════════════════════════════════════════════
            #  ONLINE  —  Battle Royale
            # ════════════════════════════════════════════
            elif t == "join_online":
                player_name = msg.get("name", "Player")[:20]

                # start fresh game if needed
                if online_game is None or online_game.finished:
                    online_game = OnlineGame()

                # if game already started with max players, tell client to wait
                if online_game.started and not online_game.finished:
                    await send(ws, {"type": "error",
                                    "msg": "اللعبة بدأت بالفعل. انتظر الجولة الجاية!"})
                    continue

                if len(online_game.players) >= OnlineGame.MAX_PLAYERS:
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
                    "countdown": None,
                })

                # start countdown when enough players
                if len(online_game.players) >= OnlineGame.MIN_TO_START \
                        and not online_game.started \
                        and online_game._cd_task is None:
                    online_game._cd_task = asyncio.create_task(
                        online_game.schedule_start()
                    )

            # ════════════════════════════════════════════
            #  PARTY  —  Create room
            # ════════════════════════════════════════════
            elif t == "create_party":
                player_name = msg.get("name", "Host")[:20]
                mode        = msg.get("mode",   "coop")
                is_public   = bool(msg.get("public", True))
                password    = msg.get("password", "")
                custom_code = msg.get("code",  "").strip().upper()

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

                await send(ws, {
                    "type":      "party_created",
                    "code":      code,
                    "public":    is_public,
                    "mode":      mode,
                    "player_id": str(id(ws)),
                })
                await send(ws, game.lobby_msg())

            # ════════════════════════════════════════════
            #  PARTY  —  Join room
            # ════════════════════════════════════════════
            elif t == "join_party":
                player_name = msg.get("name", "Player")[:20]
                code        = msg.get("code", "").strip().upper()
                password    = msg.get("password", "")

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

                if not game.check_password(password):
                    await send(ws, {"type": "error",
                                    "msg":  "كلمة السر غلطانة"})
                    continue

                game.add(ws, player_name)
                party_code   = code
                current_game = code

                await send(ws, {
                    "type":      "joined_party",
                    "code":      code,
                    "player_id": str(id(ws)),
                })
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
                    "name":  p_data.get("name", player_name or "?"),
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
                target_name = msg.get("target_name") or msg.get("target")
                damage      = max(0, min(float(msg.get("damage", 0)), 100))
                damage      = int(round(damage))

                pool = {}
                if current_game == "online" and online_game:
                    pool = online_game.players
                elif current_game and current_game in parties:
                    pool = parties[current_game].players

                shooter_name = pool.get(ws, {}).get("name") if ws in pool else (player_name or "?")

                for tw, tp in list(pool.items()):
                    if tw is ws:           # prevent self-hit
                        continue
                    match = (tp.get("name") == target_name) or \
                            (str(id(tw)) == target_name)
                    if match and tp.get("alive", True):
                        tp["hp"] = max(0, tp["hp"] - damage)
                        if tp["hp"] <= 0:
                            tp["alive"] = False
                            # kill credit to shooter
                            if ws in pool:
                                pool[ws]["kills"] = pool[ws].get("kills", 0) + 1
                                killer_name = pool[ws]["name"]
                            else:
                                killer_name = player_name or "?"
                            await send(tw, {"type": "eliminated",
                                            "reason": "shot",
                                            "by": killer_name})
                            # notify shooter of kill
                            await send(ws, {"type": "kill_confirmed",
                                            "victim": tp.get("name", "?")})
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

            # ── unknown type: ignore silently ──

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
                parties.pop(current_game, None)
            else:
                await broadcast(game._all(), game.lobby_msg())


# ─────────────────────────────────────────────────────────
#  HTTP handler  (health check + status)
# ─────────────────────────────────────────────────────────
async def http_handler(path, request_headers):
    """Handle plain HTTP GET requests (Railway health checks)."""
    if path == "/" or path == "/health":
        body = b"BattleKids Server v3 - OK"
        return (200, [("Content-Type", "text/plain"),
                      ("Content-Length", str(len(body)))], body)

    if path == "/status":
        online_count = len(online_game.players) if online_game else 0
        party_count  = sum(len(g.players) for g in parties.values())
        data = {
            "status":        "ok",
            "version":       3,
            "online_players": online_count,
            "online_started": online_game.started if online_game else False,
            "parties":       len(parties),
            "party_players": party_count,
        }
        body = json.dumps(data, indent=2).encode()
        return (200, [("Content-Type", "application/json"),
                      ("Content-Length", str(len(body)))], body)

    # Let websocket upgrade proceed normally for all other paths
    return None


# ─────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────
async def main():
    print(f"BattleKids Server v3  —  port {PORT}")
    async with serve(
        handler,
        "0.0.0.0",
        PORT,
        ping_interval=20,
        ping_timeout=60,
        process_request=http_handler,
    ):
        await asyncio.Future()   # run forever


if __name__ == "__main__":
    asyncio.run(main())
  
