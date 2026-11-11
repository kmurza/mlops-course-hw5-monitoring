# MLOps HW5: мониторинг fraud-detector

Сервис скоринга фродовых транзакций с наблюдаемостью: Prometheus собирает метрики с живого потока, Grafana строит дашборды, настроены алерты на аномалии и срез из PostgreSQL.

## Архитектура
| Откуда | Куда | Что передаётся | Транспорт |
|---|---|---|---|
| Streamlit | Kafka (`transactions`) | CSV-транзакции | Kafka producer |
| Kafka (`transactions`) | scorer | Транзакции на скоринг | Kafka consumer |
| scorer | Kafka (`scoring`) | Скоры / флаг фрода | Kafka producer |
| Kafka (`scoring`) | db_consumer | Результаты скоринга | Kafka consumer |
| db_consumer | PostgreSQL (`fraud_db.scores`) | Записи скоров | SQL INSERT |
| scorer, db_consumer, node-exporter | Prometheus | `/metrics` | HTTP scrape |
| Prometheus | Grafana | Ряды метрик | datasource |
| PostgreSQL | Grafana | Срез `scores` | SQL |
## Запуск

```bash
docker compose up -d --build
```
Подождать ~30 секунд, пока поднимутся Kafka, Airflow, scorer, db_consumer, Prometheus, Grafana.

## Доступы
| Сервис                        | URL                     | Логин  | Пароль   |
|-------------------------------|-------------------------|--------|----------|
| Grafana                       | http://localhost:3000   | admin  | Qwerty   |
| Prometheus                    | http://localhost:9090   | —      | —        |
| Streamlit (загрузка транзакций)| http://localhost:8600  | —      | —        |
| Airflow                       | http://localhost:8080   | admin  | admin    |
| Kafka UI                      | http://localhost:8601   | —      | —        |

## Метрики
| Метрика | Тип | Назначение | График / панель |
|---|---|---|---|
| `scoring_transactions_total` | Counter | Число обработанных транзакций | **Transactions per second** |
| `scoring_processing_seconds` | Histogram | Время обработки одной транзакции | **Processing time (p50/p95)** |
| `scoring_score` | Histogram | Распределение скоров | **Score distribution (heatmap)** |
| `scoring_fraud_total` | Counter | Число транзакций, помеченных как фрод | **Fraud ratio** |
| `scoring_model_reload_total{version}` | Counter | Счётчик подхватов модели; увеличивается при смене `current_model.pkl` | **Annotation «model version change»** на дашборде Scoring |
| `scoring_model_info{version}` | Gauge | Текущая версия модели (`1` у активной) | **Totals** внизу дашборда Scoring |
## Сервис db_consumer (порт 8000)
| Метрика | Тип | Назначение | График / панель |
|---|---|---|---|
| `db_records_total` | Counter | Число записанных скоров | **DB write rate** |
| `db_write_seconds` | Histogram | Время записи в PostgreSQL | **DB write time (p50/p95)** |
| `db_fraud_total` | Counter | Число записанных фрод-транзакций | **Fraud records per second** |
| `db_score` | Histogram | Распределение записанных скоров | **Written scores distribution** |

## node-exporter (порт 9100)
Стандартные метрики ОС (node_cpu_seconds_total, node_memory_*, node_network_*, node_filesystem_*).

## Дашборды
Все дашборды лежат в репозитории (monitoring/grafana/dashboards/) и подхватываются через provisioning при старте Grafana.

1. Scoring (обработка транзакций)
   - Transactions per second — TPS из scoring_transactions_total.
   - Processing time (p50/p95) — квантили scoring_processing_seconds.
   -  Fraud ratio — доля фрода (rate(scoring_fraud_total) / rate(scoring_transactions_total)).
   - Score distribution (heatmap) — распределение скоров из scoring_score.
   - Scores by model version — таблица из PostgreSQL: число скоров, число фродов, доля фрода, средний скор по каждой версии модели.
   - Totals — накопительные счётчики и текущая версия модели.
   - Annotation model version change — вертикальные линии при смене версии модели.

2. DB Writer (запись в PostgreSQL)

Скорость записи, p95/p50 времени записи, фрод-записи в секунду, heatmap записанных скоров, накопительные счётчики.

3. System (node-exporter)
CPU, RAM, network (RX/TX), disk used %.

## Алерты
Два правила в Grafana, лежат в `monitoring/grafana/provisioning/alerting/rules.yml`, появляются автоматически после docker compose up.

| Алерт | Условие (PromQL) | Порог | Как проверить срабатывание |
|---|---|---|---|
| **Fraud ratio too high** | `sum(rate(scoring_fraud_total[5m])) / sum(rate(scoring_transactions_total[5m])) > 0.5` | Доля фрода > 50% за 5 минут | Временно поменять `FRAUD_THRESHOLD=0.1` в scorer, прогнать поток 5+ минут — доля фрода станет высокой, алерт перейдёт в **Firing**. Вернуть `0.5` назад. |
| **Scoring stream stopped** | `sum(rate(scoring_transactions_total[5m])) == 0` | Нет транзакций за 5 минут | Остановить поток (не нажимать «Отправить» в Streamlit), подождать 5 минут — алерт перейдёт в **Firing**. |

Смотреть: `http://localhost:3000` → Alerting → Alert rules. Доставка в Telegram/email не настроена (по условию задания не требуется).

## Как проверить, что всё работает
1. Поднять всё:
```
bash
docker compose up -d --build
```
2. Проверить, что Prometheus видит таргеты:

   - Открыть `http://localhost:9090/targets` — все 4 таргета (`prometheus`, `node-exporter`, `scorer`, `db_consumer`) должны быть UP.

3. Прогнать поток:

   - Открыть `http://localhost:8600` (Streamlit).
   - Загрузить `train.csv` (или любой CSV с колонками транзакций).
   - Нажать «Отправить».

4. Проверить, что метрики растут:

   - Открыть `http://localhost:3000` → Dashboards → Scoring.
   - TPS должен подняться выше нуля, `Fraud ratio` — живой, heatmap заполняется.
   - Через 1–2 минуты — Totals внизу показывает накопленные счётчики.

5. Проверить смену версии модели (annotation):

   - Обновить `mtime` модели:
```
bash
touch models/current_model.pkl
"v_test_$(date +%Y%m%d_%H%M%S)" > models/current_model.pkl.version
```
   - Прогнать ещё раз поток через Streamlit (10–20 строк).
   - На дашборде Scoring появится вертикальная линия с подписью новой версии.

6. Проверить алерты:

   - Открыть `http://localhost:3000` → Alerting → Alert rules.
   - Должны быть два правила (`Fraud ratio too high`, `Scoring stream stopped`) в статусе `Normal` или `Pending`.

## Структура репозитория
```
text
mlops-course-hw5-monitoring/
├── docker-compose.yml
├── README.md
├── dags/                          # Airflow DAGs (train, publish, evaluate)
├── data/                          # train.parquet, test.parquet, model_candidate.pkl
├── models/                        # current_model.pkl, logreg_model.pkl, .version
├── scorer/
│   ├── fraud_detector/            # app.py, preprocessing.py, scorer.py
│   ├── db_consumer/               # consumer_to_db.py
│   ├── interface/                 # Streamlit app
│   └── init-db/init.sql           # Схема таблицы scores
└── monitoring/
    ├── prometheus.yml
    └── grafana/
        ├── provisioning/
        │   ├── datasources/datasource.yml
        │   ├── dashboards/dashboards.yml
        │   └── alerting/
        │       ├── contactpoints.yml
        │       └── rules.yml
        └── dashboards/
            ├── scoring.json
            ├── db_writer.json
            └── system.json
```

## Ограничения текущей реализации
Фильтры `State` / `Merchant` на дашборде присутствуют, но в текущей версии Grafana multi-value переменные не подставляются в SQL корректно (`malformed array literal` / `syntax error`). Панели дашборда работают без фильтрации по этим полям. При необходимости легко включить: перевести переменные в режим single-value и использовать us_state = '$us_state' в SQL.