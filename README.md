# Аналитическая система прогнозирования валютных курсов

## Описание
End-to-end система для сбора, обработки данных и прогнозирования валютных курсов с использованием ML.

## Технологический стек
- **Data Lake**: MinIO
- **Orchestration**: Apache Airflow
- **Processing**: Apache Spark
- **DWH**: PostgreSQL
- **ML**: Prophet, Scikit-learn
- **Dashboard**: Streamlit

## Быстрый запуск по шагам от 1 до ++ делать по порядку

### 1. Предварительные требования
- Docker & Docker Compose
- Python 3.10+

### 2. Клонирование и запуск
```bash
git clone git@github.com:pwd4/etl.git
cd currency_forecast_system
pip install -r requirements.txt
docker-compose up -d
```

### 3. Настроить pgAdmin для Postgres
- В pgAdmin, перейти на http://localhost:5050 → нажать Add New Server и добавить 
- Host postgres
- Port 5432
- Username airflow
- Password airflow


### 4. Доступы в UI
- Airflow: http://localhost:8080 (airflow/airflow)
- MinIO Console: http://localhost:9001 (minio/minio123)
- pgAdmin http://localhost:5050 (admin@admin.com/admin)

