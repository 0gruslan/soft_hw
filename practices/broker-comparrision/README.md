# Практика: сравнение RabbitMQ и Redis (Python)

Минимальный рабочий стенд:
- `producer -> broker -> consumer`
- брокеры: `RabbitMQ` и `Redis`
- одинаковые условия для обоих
- автоматический сбор метрик в `CSV` и `Markdown`

В `docker-compose.yml` для обоих контейнеров выставлены одинаковые лимиты:
- `cpus: 1.0`
- `mem_limit: 512m`

## Что внутри

- `benchmark.py` - основной скрипт нагрузочного теста.
- `docker-compose.yml` - поднимает `RabbitMQ` и `Redis` в single instance.
- `requirements.txt` - Python зависимости.
- `run_example.ps1` - пример запуска на Windows PowerShell.
- `run_benchmark_in_docker.ps1` - запуск полного бенчмарка через Docker (если в системе нет Python).
- `report_template.md` - шаблон отчета для сдачи.
- `EXPLANATION.md` - подробное объяснение кода.

## Быстрый старт

1. Поднимите брокеры:

```powershell
docker compose up -d
```

2. Установите зависимости:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3. Запустите тест:

```powershell
python benchmark.py --broker both --duration 15 --producers 1 --consumers 1 --sizes 128,1024,10240,102400 --rates 1000,5000,10000
```

4. Результаты появятся в папке `results/`:
- `results_YYYYMMDD_HHMMSS.csv`
- `summary_YYYYMMDD_HHMMSS.md`

## Что измеряется

- `messages/sec`
- `latency avg / p95 / max`
- отправлено / обработано / потеряно
- ошибки producer + consumer
- `max backlog`
- (дополнительно) `CPU` и `RAM` локальной машины

## Как понять деградацию

В поле `degraded` будет `YES`, если выполнено хотя бы одно:
- есть backlog (`max_backlog > 0`)
- есть ошибки
- есть потери сообщений (`lost_messages > 0`)

## Важно

- Размер сообщения в тесте задается через поле `payload`, итоговый JSON чуть больше указанного размера.
- Для честного сравнения не меняйте параметры между брокерами.
