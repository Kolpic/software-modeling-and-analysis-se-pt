DROP TABLE IF EXISTS kyc_verifications, trades, orders, transactions, wallets, trading_pairs, assets, users CASCADE;
DROP TYPE IF EXISTS asset_type, transaction_type, order_type, order_side, kyc_status, order_status;

CREATE TYPE asset_type AS ENUM ('crypto', 'fiat');
CREATE TYPE transaction_type AS ENUM ('deposit', 'withdrawal');
CREATE TYPE order_type AS ENUM ('market', 'limit');
CREATE TYPE order_side AS ENUM ('buy', 'sell');
CREATE TYPE order_status AS ENUM ('open', 'filled', 'canceled');
CREATE TYPE kyc_status AS ENUM ('pending', 'approved', 'rejected');

CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    country VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE assets (
    asset_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    name VARCHAR(50) NOT NULL,
    type asset_type NOT NULL
);

CREATE TABLE wallets (
    wallet_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    asset_id INT NOT NULL REFERENCES assets(asset_id),
    balance NUMERIC(20, 8) NOT NULL DEFAULT 0.0,
    CONSTRAINT user_asset_unique UNIQUE (user_id, asset_id) -- Всеки потребител има само един портфейл за даден актив
);

CREATE TABLE trading_pairs (
    pair_id SERIAL PRIMARY KEY,
    base_asset_id INT NOT NULL REFERENCES assets(asset_id),
    quote_asset_id INT NOT NULL REFERENCES assets(asset_id),
    pair_name VARCHAR(20) NOT NULL UNIQUE,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    pair_id INT NOT NULL REFERENCES trading_pairs(pair_id),
    type order_type NOT NULL,
    side order_side NOT NULL,
    amount NUMERIC(20, 8) NOT NULL,
    price NUMERIC(20, 8), -- Може да е NULL за market поръчки
    status order_status NOT NULL DEFAULT 'open',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE trades (
    trade_id SERIAL PRIMARY KEY,
    pair_id INT NOT NULL REFERENCES trading_pairs(pair_id),
    buy_order_id INT NOT NULL REFERENCES orders(order_id),
    sell_order_id INT NOT NULL REFERENCES orders(order_id),
    amount NUMERIC(20, 8) NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    asset_id INT NOT NULL REFERENCES assets(asset_id),
    type transaction_type NOT NULL,
    amount NUMERIC(20, 8) NOT NULL,
    address TEXT,
    status kyc_status NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE kyc_verifications (
    kyc_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id) UNIQUE, -- Един потребител има една верификация
    document_type VARCHAR(50),
    document_number VARCHAR(100),
    status kyc_status NOT NULL DEFAULT 'pending',
    verified_at TIMESTAMP WITH TIME ZONE
);


INSERT INTO users (username, email, password_hash, country) VALUES
('ivan_trader', 'ivan@test.com', 'hash123', 'Bulgaria'),
('maria_crypto', 'maria@test.com', 'hash456', 'Germany');

INSERT INTO assets (ticker, name, type) VALUES
('BTC', 'Bitcoin', 'crypto'),
('ETH', 'Ethereum', 'crypto'),
('USDT', 'Tether', 'crypto'),
('EUR', 'Euro', 'fiat');

INSERT INTO wallets (user_id, asset_id, balance) VALUES
(1, 1, 1.5),    -- Иван има 1.5 BTC
(1, 3, 10000),  -- Иван има 10000 USDT
(2, 2, 20.0),   -- Мария има 20 ETH
(2, 3, 50000);  -- Мария има 50000 USDT

INSERT INTO trading_pairs (base_asset_id, quote_asset_id, pair_name) VALUES
(1, 3, 'BTC/USDT'),
(2, 3, 'ETH/USDT');

INSERT INTO orders (user_id, pair_id, type, side, amount, price, status) VALUES
(1, 1, 'limit', 'sell', 0.5, 60000.00, 'filled'), -- Иван продава BTC
(2, 1, 'limit', 'buy', 0.5, 60000.00, 'filled');  -- Мария купува BTC

INSERT INTO trades (pair_id, buy_order_id, sell_order_id, amount, price) VALUES
(1, 2, 1, 0.5, 60000.00);

INSERT INTO transactions (user_id, asset_id, type, amount, status) VALUES
(1, 3, 'deposit', 10000, 'approved');

--- Тригери, Функции и Съхранени процедури ---

-- ТРИГЕР 1: Проверка на баланса преди поставяне на поръчка
CREATE OR REPLACE FUNCTION check_balance_before_order()
RETURNS TRIGGER AS $$
DECLARE
    v_asset_id_to_check INT;
    v_required_balance NUMERIC;
    v_current_balance NUMERIC;
    v_pair RECORD;
BEGIN
    -- Намираме кой актив се използва за поръчката
    SELECT base_asset_id, quote_asset_id INTO v_pair
    FROM trading_pairs WHERE pair_id = NEW.pair_id;

    IF NEW.side = 'sell' THEN
        -- Ако продава, проверяваме баланса на базовия актив (напр. BTC в BTC/USDT)
        v_asset_id_to_check := v_pair.base_asset_id;
        v_required_balance := NEW.amount;
    ELSE -- 'buy'
        -- Ако купува, проверяваме баланса на котирания актив (напр. USDT в BTC/USDT)
        v_asset_id_to_check := v_pair.quote_asset_id;
        v_required_balance := NEW.amount * NEW.price; -- При limit order
    END IF;

    -- Взимаме текущия баланс
    SELECT balance INTO v_current_balance
    FROM wallets
    WHERE user_id = NEW.user_id AND asset_id = v_asset_id_to_check;

    IF v_current_balance IS NULL OR v_current_balance < v_required_balance THEN
        RAISE EXCEPTION 'Insufficient funds to place the order.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_before_insert_order
BEFORE INSERT ON orders
FOR EACH ROW
EXECUTE FUNCTION check_balance_before_order();

-- ТРИГЕР 2: Актуализиране на баланса след изпълнена сделка (опростен вариант)
CREATE OR REPLACE FUNCTION update_balances_after_trade()
RETURNS TRIGGER AS $$
DECLARE
    v_sell_order RECORD;
    v_buy_order RECORD;
    v_pair RECORD;
BEGIN
    -- Взимаме информация за поръчките
    SELECT user_id, pair_id INTO v_sell_order FROM orders WHERE order_id = NEW.sell_order_id;
    SELECT user_id, pair_id INTO v_buy_order FROM orders WHERE order_id = NEW.buy_order_id;
    
    -- Взимаме информация за активите в двойката
    SELECT base_asset_id, quote_asset_id INTO v_pair FROM trading_pairs WHERE pair_id = NEW.pair_id;

    -- Намаляваме баланса на продавача (продава base asset)
    UPDATE wallets SET balance = balance - NEW.amount 
    WHERE user_id = v_sell_order.user_id AND asset_id = v_pair.base_asset_id;
    
    -- Увеличаваме баланса на продавача (получава quote asset)
    UPDATE wallets SET balance = balance + (NEW.amount * NEW.price)
    WHERE user_id = v_sell_order.user_id AND asset_id = v_pair.quote_asset_id;

    -- Намаляваме баланса на купувача (плаща с quote asset)
    UPDATE wallets SET balance = balance - (NEW.amount * NEW.price)
    WHERE user_id = v_buy_order.user_id AND asset_id = v_pair.quote_asset_id;
    
    -- Увеличаваме баланса на купувача (получава base asset)
    UPDATE wallets SET balance = balance + NEW.amount
    WHERE user_id = v_buy_order.user_id AND asset_id = v_pair.base_asset_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_after_insert_trade
AFTER INSERT ON trades
FOR EACH ROW
EXECUTE FUNCTION update_balances_after_trade();


-- ФУНКЦИЯ 1: Изчисляване на общата стойност на портфейла на потребител в USDT
CREATE OR REPLACE FUNCTION get_user_portfolio_value_in_usdt(p_user_id INT)
RETURNS NUMERIC AS $$
DECLARE
    total_value NUMERIC := 0;
    wallet_record RECORD;
    asset_price NUMERIC;
BEGIN
    FOR wallet_record IN SELECT w.balance, a.ticker FROM wallets w JOIN assets a ON w.asset_id = a.asset_id WHERE w.user_id = p_user_id
    LOOP
        IF wallet_record.ticker = 'USDT' THEN
            asset_price := 1;
        ELSE
            -- В реална система тук ще има извикване на цена от trading_pairs/trades
            -- За простота, ползваме твърдо кодирани цени
            IF wallet_record.ticker = 'BTC' THEN asset_price := 60000;
            ELSIF wallet_record.ticker = 'ETH' THEN asset_price := 3000;
            ELSE asset_price := 0;
            END IF;
        END IF;
        total_value := total_value + (wallet_record.balance * asset_price);
    END LOOP;

    RETURN total_value;
END;
$$ LANGUAGE plpgsql;

-- ФУНКЦИЯ 2: Връщане на историята на сделките за дадена търговска двойка
CREATE OR REPLACE FUNCTION get_trades_for_pair(p_pair_name VARCHAR)
RETURNS TABLE(trade_time TIMESTAMP WITH TIME ZONE, trade_amount NUMERIC, trade_price NUMERIC) AS $$
BEGIN
    RETURN QUERY
    SELECT t.executed_at, t.amount, t.price
    FROM trades t
    JOIN trading_pairs tp ON t.pair_id = tp.pair_id
    WHERE tp.pair_name = p_pair_name
    ORDER BY t.executed_at DESC;
END;
$$ LANGUAGE plpgsql;


-- СЪХРАНЕНА ПРОЦЕДУРА 1: Поставяне на пазарна поръчка за покупка
CREATE OR REPLACE PROCEDURE place_market_buy_order(
    p_user_id INT,
    p_pair_name VARCHAR,
    p_amount_to_spend NUMERIC -- колко USDT/EUR искаме да похарчим
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_pair RECORD;
    v_best_price NUMERIC;
    v_amount_to_buy NUMERIC;
BEGIN
    -- Намираме двойката и най-добрата цена (симулираме с последната сделка)
    SELECT pair_id, base_asset_id, quote_asset_id INTO v_pair FROM trading_pairs WHERE pair_name = p_pair_name;
    SELECT price INTO v_best_price FROM trades WHERE pair_id = v_pair.pair_id ORDER BY executed_at DESC LIMIT 1;
    
    IF v_best_price IS NULL THEN
        RAISE EXCEPTION 'No price available for this pair.';
    END IF;

    v_amount_to_buy := p_amount_to_spend / v_best_price;
    
    INSERT INTO orders(user_id, pair_id, type, side, amount, price, status)
    VALUES (p_user_id, v_pair.pair_id, 'market', 'buy', v_amount_to_buy, v_best_price, 'open');

    COMMIT;
END;
$$;


-- СЪХРАНЕНА ПРОЦЕДУРА 2: Одобряване на KYC верификация
CREATE OR REPLACE PROCEDURE approve_kyc(p_user_id INT)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE kyc_verifications
    SET status = 'approved',
        verified_at = NOW()
    WHERE user_id = p_user_id AND status = 'pending';

    IF NOT FOUND THEN
        RAISE NOTICE 'No pending KYC found for user ID %', p_user_id;
    END IF;
    
    COMMIT;
END;
$$;

CREATE SCHEMA dw;

-- Измерение за потребители
CREATE TABLE dw.dim_user (
    user_key SERIAL PRIMARY KEY,
    user_id INT NOT NULL, -- От оригиналната таблица
    username VARCHAR(50),
    country VARCHAR(50),
    registration_date DATE
);

-- Измерение за търговски двойки
CREATE TABLE dw.dim_trading_pair (
    pair_key SERIAL PRIMARY KEY,
    pair_id INT NOT NULL, -- От оригиналната таблица
    pair_name VARCHAR(20),
    base_asset_ticker VARCHAR(10),
    quote_asset_ticker VARCHAR(10)
);

-- Измерение за дати (много важна таблица в DW)
CREATE TABLE dw.dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    day INT,
    day_of_week_name VARCHAR(10)
);

CREATE TABLE dw.fact_trades (
    trade_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dw.dim_date(date_key),
    buyer_user_key INT REFERENCES dw.dim_user(user_key),
    seller_user_key INT REFERENCES dw.dim_user(user_key),
    pair_key INT REFERENCES dw.dim_trading_pair(pair_key),
    -- Мерки (числовите данни)
    amount_traded NUMERIC(20, 8),
    price NUMERIC(20, 8),
    total_trade_value NUMERIC(20, 8) -- Това е нова, изчислена метрика
);

-- Попълване на dim_user
INSERT INTO dw.dim_user (user_id, username, country, registration_date)
SELECT user_id, username, country, created_at::DATE
FROM public.users;

-- Попълване на dim_trading_pair
INSERT INTO dw.dim_trading_pair (pair_id, pair_name, base_asset_ticker, quote_asset_ticker)
SELECT 
    tp.pair_id,
    tp.pair_name,
    base_asset.ticker,
    quote_asset.ticker
FROM public.trading_pairs tp
JOIN public.assets base_asset ON tp.base_asset_id = base_asset.asset_id
JOIN public.assets quote_asset ON tp.quote_asset_id = quote_asset.asset_id;

-- Попълване на dim_date (с данни за една година напред)
INSERT INTO dw.dim_date (full_date, year, quarter, month, day, day_of_week_name)
SELECT
    d,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    EXTRACT(DAY FROM d),
    TO_CHAR(d, 'Day')
FROM generate_series('2023-01-01'::DATE, '2025-12-31'::DATE, '1 day') AS d;

-- Попълване на fact_trades
INSERT INTO dw.fact_trades (date_key, buyer_user_key, seller_user_key, pair_key, amount_traded, price, total_trade_value)
SELECT
    -- Връзки към измеренията
    d.date_key,
    buyer_dim.user_key,
    seller_dim.user_key,
    pair_dim.pair_key,
    -- Мерки
    t.amount,
    t.price,
    t.amount * t.price AS total_trade_value -- Трансформация
FROM public.trades t
-- Join-ваме с оригиналните таблици, за да намерим ID-тата
JOIN public.orders buy_order ON t.buy_order_id = buy_order.order_id
JOIN public.orders sell_order ON t.sell_order_id = sell_order.order_id
-- Join-ваме с новите DW таблици, за да намерим техните ключове
JOIN dw.dim_date d ON t.executed_at::DATE = d.full_date
JOIN dw.dim_user buyer_dim ON buy_order.user_id = buyer_dim.user_id
JOIN dw.dim_user seller_dim ON sell_order.user_id = seller_dim.user_id
JOIN dw.dim_trading_pair pair_dim ON t.pair_id = pair_dim.pair_id;