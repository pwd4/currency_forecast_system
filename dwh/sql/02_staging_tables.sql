-- Таблица для хранения курсов валют ЦБ РФ
CREATE TABLE IF NOT EXISTS stg.cbr_currency_rates (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    currency_name VARCHAR(100) NOT NULL,
    nominal INTEGER NOT NULL,
    value NUMERIC(10, 4) NOT NULL,
    vunit_rate NUMERIC(10, 4),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cbr_rates_date ON stg.cbr_currency_rates(date);
CREATE INDEX IF NOT EXISTS idx_cbr_rates_currency ON stg.cbr_currency_rates(currency_code);

-- Таблица для драгоценных металлов
CREATE TABLE IF NOT EXISTS stg.cbr_metals (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    metal_code INTEGER NOT NULL,
    metal_name VARCHAR(50) NOT NULL,
    buy_price NUMERIC(10, 2),
    sell_price NUMERIC(10, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для нефти Brent
CREATE TABLE IF NOT EXISTS stg.brent_oil (
    id SERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    open_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    volume INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);