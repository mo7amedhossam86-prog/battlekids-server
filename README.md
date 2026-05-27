# BattleKids Server — خطوات الرفع على Railway

## الخطوات (5 دقايق)

### 1. اعمل حساب على Railway
اذهب إلى: https://railway.app
سجّل بـ GitHub أو Google

### 2. ارفع المجلد ده
- افتح https://railway.app/new
- اختر "Deploy from GitHub" أو "Empty Project"
- لو GitHub: ارفع المجلد ده كـ repo جديد وربطه
- لو Empty: اختر "Deploy" ثم ارفع الملفات

### 3. بعد الرفع
- Railway هيديك URL شكله:  wss://xxxx.up.railway.app
- افتح ملف  server_config.json  اللي جنب اللعبة
- غيّر السطر ده:
    "server_url": "wss://xxxx.up.railway.app"

### 4. شغّل اللعبة — خلاص!
اللعبة هتقرأ الـ URL الجديد تلقائياً.

## ملفات المجلد
- server.py         — السيرفر الأساسي
- requirements.txt  — websockets
- Procfile          — أمر التشغيل
- railway.json      — إعدادات Railway

## ملاحظة
Railway بيديك $5 رصيد مجاني كل شهر — كافي للعب.
