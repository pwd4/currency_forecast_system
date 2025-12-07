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

## Быстрый запуск

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

### 3. Доступы в UI
- Airflow: http://localhost:8080 (admin/admin)
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
- Streamlit Dashboard: http://localhost:8501
- PostgreSQL: localhost:5432