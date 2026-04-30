# Run benchmark completely via Docker (no local Python required)

docker compose up -d

docker run --rm --network broker-comparrision_default -v "${PWD}:/work" -w /work python:3.11-slim sh -c "pip install -r requirements.txt && python benchmark.py --broker both --duration 10 --producers 1 --consumers 1 --sizes 128,1024,10240,102400 --rates 1000,5000,10000 --rabbit-host rabbitmq --redis-host redis"

# Optional: stop containers after run
# docker compose down
