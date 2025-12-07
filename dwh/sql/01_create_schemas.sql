-- Создание схем для многослойного хранилища
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS dm;

COMMENT ON SCHEMA stg IS 'Staging layer: сырые данные из источников';
COMMENT ON SCHEMA dwh IS 'Core DWH: очищенные данные в звездообразной схеме';
COMMENT ON SCHEMA dm IS 'Data Marts: витрины для аналитики и дашборда';