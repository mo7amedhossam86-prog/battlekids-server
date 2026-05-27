# BattleKids Server — v3

## ✅ إيه اللي اتصلح في v3

| المشكلة | الحل |
|---------|------|
| Zone damage كانت 1 HP/tick (بطيء جداً) | صارت 9 HP/s تمام زي الكلاينت |
| Online game مش بيتعمل reset بعد ما تخلص | بيعمل OnlineGame جديدة تلقائياً |
| مفيش HTTP health check لـ Railway | `/` و `/health` بيرجعوا 200 OK |
| مفيش `/status` للـ debugging | `/status` بيرجع JSON بعدد اللاعبين والغرف |
| Countdown مش موجود قبل بداية اللعبة | 10 ثواني countdown مع broadcast |
| Party loop مش بيبعت zone data | الـ state message دلوقتي بيتضمن zone_r, zone_cx, zone_cy |
| Kill notification مش بتوصل للـ shooter | بيبعت `kill_confirmed` مع اسم الضحية |
| Double-join ممكن يحصل | Guard في `add()` بيمنع اللاعب يدخل مرتين |

## الـ Endpoints

| Path | استخدام |
|------|---------|
| `ws://...` | WebSocket — اللعبة |
| `GET /` | Health check — Railway |
| `GET /health` | Health check |
| `GET /status` | JSON: عدد اللاعبين والغرف الحالية |

## رفع على Railway (5 دقايق)

### 1. اعمل حساب
https://railway.app — سجّل بـ GitHub

### 2. ارفع المجلد
- اذهب إلى https://railway.app/new
- اختر **Deploy from GitHub** أو **Empty Project**
- ارفع الملفات الأربعة: `server.py`, `requirements.txt`, `Procfile`, `railway.json`

### 3. بعد الرفع
Railway هيديك URL شكله: `wss://xxxx.up.railway.app`

افتح `server_config.json` جنب اللعبة وغيّر:
```json
{
  "server_url": "wss://xxxx.up.railway.app"
}
```

### 4. تأكد إنه شغال
افتح في المتصفح: `https://xxxx.up.railway.app/status`
المفروض يظهر:
```json
{
  "status": "ok",
  "version": 3,
  "online_players": 0,
  ...
}
```

## ملفات المجلد
| ملف | وظيفته |
|-----|--------|
| `server.py` | السيرفر الأساسي |
| `requirements.txt` | `websockets>=12.0` |
| `Procfile` | أمر التشغيل لـ Railway |
| `railway.json` | إعدادات البناء |
| `server_config.json` | URL السيرفر (بجانب اللعبة مش هنا) |

## ملاحظة
Railway بيديك $5 رصيد مجاني كل شهر — كافي للعب عادي.
