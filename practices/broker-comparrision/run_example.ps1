# 1) start brokers
docker compose up -d

# 2) python env (first run)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 3) run benchmark
python benchmark.py --broker both --duration 15 --producers 1 --consumers 1 --sizes 128,1024,10240,102400 --rates 1000,5000,10000

# 4) stop brokers (after tests)
# docker compose down