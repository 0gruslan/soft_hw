# Dating App Backend

Stage 2 and Stage 3 implementation is ready.

## Implemented Services

- `user_service` on port `8000`
- `profile_service` on port `8001`
- `interaction_service` on port `8002`
- `ranking_service` on port `8003`
- `bot_service` (Telegram bot)

## Stage 3 Coverage

- Profile CRUD:
- `POST /profiles`
- `GET /profiles/{profile_id}`
- `GET /profiles/by-user/{user_id}`
- `PUT /profiles/by-user/{user_id}`
- `DELETE /profiles/by-user/{user_id}`

- Ranking algorithm:
- Level 1 (base): profile completeness, photo count, preference match
- Level 2 (behavior): like/skip ratio, mutual likes rate, hour activity ratio
- Level 3 (combined): `total_score = 0.4 * base_score + 0.6 * behavior_score`

- Caching:
- Redis queue for ranked profiles
- Prefetch `10` profiles (configurable in `.env`)

- Bot integration:
- direct HTTP integration with all services
- commands for profile management, feed, like, skip

## Environment

Each service has its own `.env` file.

- `services/user_service/.env`
- `services/profile_service/.env`
- `services/interaction_service/.env`
- `services/ranking_service/.env`
- `services/bot_service/.env`

You can create them from examples:

```powershell
Copy-Item C:\Users\gunba\Desktop\soft_hw\services\user_service\.env.example C:\Users\gunba\Desktop\soft_hw\services\user_service\.env
Copy-Item C:\Users\gunba\Desktop\soft_hw\services\profile_service\.env.example C:\Users\gunba\Desktop\soft_hw\services\profile_service\.env
Copy-Item C:\Users\gunba\Desktop\soft_hw\services\interaction_service\.env.example C:\Users\gunba\Desktop\soft_hw\services\interaction_service\.env
Copy-Item C:\Users\gunba\Desktop\soft_hw\services\ranking_service\.env.example C:\Users\gunba\Desktop\soft_hw\services\ranking_service\.env
Copy-Item C:\Users\gunba\Desktop\soft_hw\services\bot_service\.env.example C:\Users\gunba\Desktop\soft_hw\services\bot_service\.env
```

Required key in bot env:

- `TELEGRAM_BOT_TOKEN=...`

## Redis

Run Redis:

```powershell
cd C:\Users\gunba\Desktop\soft_hw
docker compose up -d redis
```

## Run Services

### 1. User Service

```powershell
cd C:\Users\gunba\Desktop\soft_hw\services\user_service
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### 2. Profile Service

```powershell
cd C:\Users\gunba\Desktop\soft_hw\services\profile_service
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn main:app --reload --port 8001
```

### 3. Interaction Service

```powershell
cd C:\Users\gunba\Desktop\soft_hw\services\interaction_service
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn main:app --reload --port 8002
```

### 4. Ranking Service

```powershell
cd C:\Users\gunba\Desktop\soft_hw\services\ranking_service
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn main:app --reload --port 8003
```

### 5. Bot Service

```powershell
cd C:\Users\gunba\Desktop\soft_hw\services\bot_service
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python bot.py
```

## Bot Commands

- `/start`
- `/help`
- `/set_profile age|gender|city|bio|interests|photo_count|preferred_gender|preferred_age_min|preferred_age_max|preferred_city`
- `/my_profile`
- `/delete_profile`
- `/feed`
- `/like`
- `/skip`

## Minimal Flow

1. Start all services and Redis.
2. Open bot chat and send `/start`.
3. Fill profile with `/set_profile`.
4. Add at least 2 users with profiles.
5. Use `/feed`, then `/like` or `/skip`.
