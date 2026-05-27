"""
BattleKids Game Server  —  v4

الإصلاحات عن v3:
  FIX-1: process_request signature صح لـ websockets v12+
          (connection, request) مش (path, request_headers)
  FIX-2: hit message — الكلاينت مش بيبعته، السيرفر دلوقتي بيحسب الضرر
          من player_update مباشرة (بيقارن hp قبل وبعد)
  FIX-3: countdown — لو اللاعبين نزلوا لـ 0 أثناء الـ countdown يتلغى
          ولو فضل لاعب واحد بس مش بيبدأ لعبة
  FIX-4: party cleanup — بعد ما game_over تتبعت، الغرفة بتتمسح من
          الذاكرة بعد 60 ثانية حتى لو اللاعبين فضلوا connected
  FIX-5: kill_confirmed بيتبعت من player_update مش من hit
          (عشان يتوافق مع الكلاينت اللي مش بيبعت hit)
"""

import asyncio, json, random, string, time, os, hashlib
import websockets
from websockets.server import serve

PORT = int(os.environ.get("PORT", 8765))

MAP_W, MAP_H = 3200, 3200

# ─────────────────────────────────────────────────────────
#  Global state
# ─────────────────────────────────────────────────────────
online_game = None
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
        "kills":    0,
        "alive":    True,
        "angle":    0,
    }


def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()[:16] if pw else ""


async def broadcast(clients, msg):
    data = json.dumps(msg)
    coros = [ws.send(data) for ws in list(clients)]
    if coros:
        await asyncio.gather(*coros, return_exceptions=True)


async def safe_send(ws, msg):
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
    COUNTDOWN_SECS = 10
    ZONE_START     = MAP_W * 0.45
    ZONE_MIN       = 150
    ZONE_SPEED     = 0.8
    ZONE_DAMAGE    = 9.0   # HP/sec

    def __init__(self):
        self.players    = {}   # ws -> player_data
        self.started    = False
        self.finished   = False
        self.zone_r     = self.ZONE_START
        self.zone_cx    = MAP_W // 2
        self.zone_cy    = MAP_H // 2
        self.winner     = None
        self._task      = None
        self._cd_task   = None

    def add(self, ws, name):
        if len(self.players) >= self.MAX_PLAYERS or ws in self.players:
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

    # FIX-3: countdown يتلغى لو اللاعبين نقصوا، ومش بيبدأ لو لاعب واحد بس
    async def schedule_start(self):
        for remaining in range(self.COUNTDOWN_SECS, 0, -1):
            n = len(self.players)
            if n < self.MIN_TO_START:
                # مفيش كفاية لاعبين — ألغي الـ countdown
                self._cd_task = None
                return
            await broadcast(self._all(), {
                "type":      "player_joined",
                "players":   n,
                "countdown": remaining,
            })
            await asyncio.sleep(1)

        # تأكد إن لسه في لاعبين كفاية
        if len(self.players) >= self.MIN_TO_START:
            await self.start()
        self._cd_task = None

    async def start(self):
        if self.started:
            return
        self.started = True
        await broadcast(self._all(), {"type": "game_start", "mode": "online"})
        self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        TICK = 0.05
        while self.players and not self.finished:
            await asyncio.sleep(TICK)

            if self.zone_r > self.ZONE_MIN:
                self.zone_r = max(self.ZONE_MIN,
                                  self.zone_r - self.ZONE_SPEED * TICK)

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
                        await safe_send(ws, {"type": "eliminated", "reason": "zone"})

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

    def add(self, ws, name):
        if ws in self.players:
            return
        p = make_player(name)
        p["id"] = str(id(ws))
        self.players[ws] = p

    def remove(self, ws):
        self.players.pop(ws, None)
        if ws == self.host and self.players:
            self.host = next(iter(self.players))

    def check_password(self, pw: str) -> bool:
        return (not self.pw_hash) or (hash_pw(pw) == self.pw_hash)

    def _all(self):
        return set(self.players.keys())

    def lobby_msg(self):
        host_name = self.players[self.host]["name"] if self.host in self.players else ""
        return {
            "type":      "lobby_state",
            "code":      self.code,
            "players":   [p["name"] for p in self.players.values()],
            "host":      host_name,
            "started":   self.started,
            "mode":      self.mode,
            "is_public": self.is_public,
        }

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
        TICK = 0.05
        while self.players and not self.finished:
            await asyncio.sleep(TICK)

            if self.mode in ("pvp", "coop"):
                if self.zone_r > self.ZONE_MIN:
                    self.zone_r = max(self.ZONE_MIN,
                                      self.zone_r - self.ZONE_SPEED * TICK)
                for ws, p in list(self.players.items()):
                    if not p.get("alive", True):
                        continue
                    dx = p["x"] - self.zone_cx
                    dy = p["y"] - self.zone_cy
                    if (dx*dx + dy*dy) ** 0.5 > self.zone_r:
                        p["hp"] = max(0, p["hp"] - self.ZONE_DAMAGE * TICK)
                        if p["hp"] <= 0:
                            p["alive"] = False
                            await safe_send(ws, {"type": "eliminated", "reason": "zone"})

            if self.mode == "pvp":
                alive = [(ws, p) for ws, p in self.players.items()
                         if p.get("alive", True)]
                if len(alive) == 1:
                    self.finished = True
                    await broadcast(self._all(),
                                    {"type": "game_over", "winner": alive[0][1]["name"]})
                    # FIX-4: cleanup بعد 60 ثانية
                    asyncio.create_task(self._cleanup_later())
                    return
                elif len(alive) == 0:
                    self.finished = True
                    await broadcast(self._all(),
                                    {"type": "game_over", "winner": None})
                    asyncio.create_task(self._cleanup_later())
                    return

            await broadcast(self._all(), {
                "type":    "state",
                "players": {str(id(ws)): p for ws, p in self.players.items()},
                "zone_r":  self.zone_r,
                "zone_cx": self.zone_cx,
                "zone_cy": self.zone_cy,
            })

    # FIX-4: تمسح الغرفة من parties بعد ما اللعبة تخلص بـ 60 ثانية
    async def _cleanup_later(self):
        await asyncio.sleep(60)
        parties.pop(self.code, None)


# ─────────────────────────────────────────────────────────
#  FIX-1: HTTP handler — websockets v12+ signature
# ─────────────────────────────────────────────────────────
async def http_handler(connection, request):
    """
    websockets v12+ بيبعت (connection, request) مش (path, headers).
    بنتحقق من request.path.
    """
    path = request.path

    if path in ("/", "/health"):
        from websockets.http11 import Response
        body = b"BattleKids Server v4 - OK"
        headers = [
            ("Content-Type",   "text/plain"),
            ("Content-Length", str(len(body))),
        ]
        return connection.respond(Response(200, "OK", headers, body))

    if path == "/status":
        from websockets.http11 import Response
        online_count = len(online_game.players) if online_game else 0
        party_count  = sum(len(g.players) for g in parties.values())
        data = {
            "status":         "ok",
            "version":        4,
            "online_players": online_count,
            "online_started": online_game.started if online_game else False,
            "parties":        len(parties),
            "party_players":  party_count,
        }
        body = json.dumps(data, indent=2).encode()
        headers = [
            ("Content-Type",   "application/json"),
            ("Content-Length", str(len(body))),
        ]
        return connection.respond(Response(200, "OK", headers, body))

    # أي path تاني → اتفض للـ WebSocket upgrade
    return None


# ─────────────────────────────────────────────────────────
#  WebSocket handler
# ─────────────────────────────────────────────────────────
async def handler(ws):
    global online_game

    player_name  = None
    current_game = None
    party_code   = None

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            t = msg.get("type", "")

            # ── PING ──
            if t == "ping":
                await safe_send(ws, {"type": "pong"})

            # ── LIST PUBLIC PARTIES ──
            elif t == "list_parties":
                public = [
                    {"code": c,
                     "host": g.players[g.host]["name"] if g.host in g.players else "?",
                     "players": len(g.players),
                     "mode": g.mode}
                    for c, g in parties.items()
                    if not g.started and g.is_public
                ]
                await safe_send(ws, {"type": "parties_list", "parties": public})

            # ── DELETE PARTY ──
            elif t == "delete_party":
                if party_code and party_code in parties:
                    game = parties[party_code]
                    if ws == game.host:
                        await broadcast(game._all(),
                                        {"type": "party_deleted", "code": party_code})
                        parties.pop(party_code, None)
                        party_code   = None
                        current_game = None

            # ── ONLINE JOIN ──
            elif t == "join_online":
                player_name = msg.get("name", "Player")[:20]

                if online_game is None or \
                   online_game.finished or \
                   len(online_game.players) >= OnlineGame.MAX_PLAYERS or \
                   online_game.started:
                    online_game = OnlineGame()

                online_game.add(ws, player_name)
                current_game = "online"

                await safe_send(ws, {
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
                        and not online_game.started \
                        and online_game._cd_task is None:
                    online_game._cd_task = asyncio.create_task(
                        online_game.schedule_start()
                    )

            # ── PARTY CREATE ──
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
                                 mode=mode, is_public=is_public, password=password)
                parties[code] = game
                party_code    = code
                current_game  = code

                await safe_send(ws, {
                    "type":      "party_created",
                    "code":      code,
                    "public":    is_public,
                    "mode":      mode,
                    "player_id": str(id(ws)),
                })
                await safe_send(ws, game.lobby_msg())

            # ── PARTY JOIN ──
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
                        await safe_send(ws, {"type": "error",
                                             "msg": "مفيش غرف عامة متاحة. اعمل واحدة!"})
                        continue
                elif code not in parties:
                    await safe_send(ws, {"type": "error", "msg": "كود الغرفة غلط"})
                    continue
                else:
                    game = parties[code]

                if game.started:
                    await safe_send(ws, {"type": "error", "msg": "اللعبة بدأت بالفعل"})
                    continue

                if not game.check_password(password):
                    await safe_send(ws, {"type": "error", "msg": "كلمة السر غلطانة"})
                    continue

                game.add(ws, player_name)
                party_code   = code
                current_game = code

                await safe_send(ws, {
                    "type":      "joined_party",
                    "code":      code,
                    "player_id": str(id(ws)),
                })
                await broadcast(game._all(), game.lobby_msg())

            # ── PARTY START ──
            elif t == "start_party":
                if party_code and party_code in parties:
                    game = parties[party_code]
                    if ws == game.host and not game.started:
                        await game.start()

            # ── PARTY SET MODE ──
            elif t == "set_mode":
                if party_code and party_code in parties:
                    game = parties[party_code]
                    if ws == game.host:
                        game.mode = msg.get("mode", game.mode)
                        await broadcast(game._all(), game.lobby_msg())

            # ── PLAYER UPDATE ──
            # FIX-2 + FIX-5: نتابع hp من هنا ونبعت kill_confirmed
            elif t == "player_update":
                p_data = msg.get("player", {})
                new_hp    = p_data.get("hp",    100)
                new_alive = p_data.get("alive", True)

                pool = None
                if current_game == "online" and online_game \
                        and ws in online_game.players:
                    pool = online_game.players
                elif current_game and current_game in parties:
                    game = parties[current_game]
                    if ws in game.players:
                        pool = game.players

                if pool is not None and ws in pool:
                    old_alive = pool[ws].get("alive", True)
                    pool[ws].update({
                        "x":     p_data.get("x",     pool[ws]["x"]),
                        "y":     p_data.get("y",     pool[ws]["y"]),
                        "angle": p_data.get("angle", pool[ws]["angle"]),
                        "hp":    new_hp,
                        "alive": new_alive,
                        "name":  p_data.get("name", pool[ws]["name"]),
                    })
                    # لو اللاعب مات (alive انقلبت من True لـ False) →
                    # يعني حد ضربه، نديه credit للي مقرب مكانه
                    if old_alive and not new_alive:
                        # دور على أقرب لاعب حي غيره (المرجح إنه الـ killer)
                        me = pool[ws]
                        closest_ws = None
                        closest_d  = float("inf")
                        for other_ws, other_p in pool.items():
                            if other_ws == ws or not other_p.get("alive", True):
                                continue
                            d = ((other_p["x"] - me["x"])**2 +
                                 (other_p["y"] - me["y"])**2) ** 0.5
                            if d < closest_d:
                                closest_d  = d
                                closest_ws = other_ws
                        if closest_ws:
                            pool[closest_ws]["kills"] = \
                                pool[closest_ws].get("kills", 0) + 1
                            await safe_send(closest_ws, {
                                "type":   "kill_confirmed",
                                "victim": me["name"],
                            })

            # ── BULLET ──
            elif t == "bullet":
                b = msg.get("bullet", {})
                b["owner"] = str(id(ws))
                out = {"type": "bullet", "bullet": b}
                if current_game == "online" and online_game:
                    await broadcast(online_game._all() - {ws}, out)
                elif current_game and current_game in parties:
                    await broadcast(parties[current_game]._all() - {ws}, out)

            # ── GRENADE ──
            elif t == "grenade":
                g = msg.get("grenade", {})
                g["owner"] = str(id(ws))
                out = {"type": "grenade", "grenade": g}
                if current_game == "online" and online_game:
                    await broadcast(online_game._all() - {ws}, out)
                elif current_game and current_game in parties:
                    await broadcast(parties[current_game]._all() - {ws}, out)

            # ── HIT (legacy — كان موجود في v2/v3 ولسه بيشتغل لو الكلاينت بعته) ──
            elif t == "hit":
                target_name = msg.get("target_name") or msg.get("target")
                damage      = max(0, min(int(msg.get("damage", 0)), 100))
                pool = {}
                if current_game == "online" and online_game:
                    pool = online_game.players
                elif current_game and current_game in parties:
                    pool = parties[current_game].players
                for tw, tp in pool.items():
                    if (tp.get("name") == target_name or str(id(tw)) == target_name) \
                            and tp.get("alive", True):
                        tp["hp"] = max(0, tp["hp"] - damage)
                        if tp["hp"] <= 0:
                            tp["alive"] = False
                            if ws in pool:
                                pool[ws]["kills"] = pool[ws].get("kills", 0) + 1
                            await safe_send(tw, {"type": "eliminated",
                                                 "reason": "shot",
                                                 "by": player_name or "?"})
                            await safe_send(ws, {"type": "kill_confirmed",
                                                 "victim": tp.get("name", "?")})
                        break

            # ── CHAT ──
            elif t == "chat":
                text = str(msg.get("text", ""))[:100]
                out  = {"type": "chat", "name": player_name or "?", "text": text}
                if current_game == "online" and online_game:
                    await broadcast(online_game._all(), out)
                elif current_game and current_game in parties:
                    await broadcast(parties[current_game]._all(), out)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
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
#  Entry point
# ─────────────────────────────────────────────────────────
async def main():
    print(f"BattleKids Server v4  —  port {PORT}")
    async with serve(
        handler,
        "0.0.0.0",
        PORT,
        ping_interval=20,
        ping_timeout=60,
        process_request=http_handler,
    ):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
