-- Сравнение торговых сделок из двух источников данных (счёт 1233 из money.tmm_small и public.de_1_39_2)
-- Задача: аггрегировать и сравнить соответствующие сделки, учитывая возможные расхождения по времени записи, ценам и т.д.


WITH tmm_bounds AS (
    -- Определяем общий временной диапазон сделок по модели
    SELECT
        MIN(open_time + INTERVAL '3 hours') AS min_open_msk,
        MAX(close_time + INTERVAL '3 hours') AS max_close_msk
    FROM money.tmm_small
    WHERE model_name LIKE '%1233%'
),

-- Агрегируем трейды из скринера в сделку (по символу и времени закрытия)
scr_deals AS (
    SELECT
        symbol,
        MIN(entry_time) AS scr_start_time,  -- начало сделки (устойчиво к разным записям времени)
        MAX(close_time) AS scr_exit_time,   -- конец сделки
        ARRAY_AGG(DISTINCT entry_price)::text[] AS entry_prices,
        COUNT(*) AS scr_trade_count
    FROM public.de_1_39_2
    WHERE close_time IS NOT NULL
      AND close_time >= (SELECT min_open_msk FROM tmm_bounds)
      AND entry_time <= (SELECT max_close_msk FROM tmm_bounds)
    GROUP BY symbol, close_time
),

-- Сделки из TMM
tmm_deals AS (
    SELECT
        symbol,
        open_time + INTERVAL '3 hours' AS open_time_msk,
        close_time + INTERVAL '3 hours' AS close_time_msk,
        avg_price_entry,
        orders
    FROM money.tmm_small
    WHERE model_name LIKE '%1233%'
      AND open_time + INTERVAL '3 hours' >= (SELECT min_open_msk FROM tmm_bounds)
      AND close_time + INTERVAL '3 hours' <= (SELECT max_close_msk FROM tmm_bounds)
),

-- Фильтруем сделки скринера по диапазону модели
filtered_scr AS (
    SELECT s.*
    FROM scr_deals s
    JOIN tmm_bounds tb
      ON s.scr_exit_time >= tb.min_open_msk
     AND s.scr_start_time <= tb.max_close_msk
),

-- Фильтруем сделки tmm по диапазону скринера
filtered_tmm AS (
    SELECT t.*
    FROM tmm_deals t
    JOIN (
        SELECT
            MIN(scr_start_time) AS min_scr_start,
            MAX(scr_exit_time)  AS max_scr_exit
        FROM filtered_scr
    ) scr_bounds
      ON t.close_time_msk >= scr_bounds.min_scr_start
     AND t.open_time_msk  <= scr_bounds.max_scr_exit
),

-- Объединяем сделки (по символу и близости во времени)
joined_deals AS (
    SELECT
        COALESCE(s.symbol, t.symbol) AS symbol,
        s.scr_start_time,
        s.scr_exit_time,
        t.open_time_msk,
        t.close_time_msk,
        ABS(EXTRACT(EPOCH FROM (t.open_time_msk - s.scr_start_time))) AS time_diff,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(s.symbol, t.symbol),
                         s.scr_start_time, s.scr_exit_time
            ORDER BY ABS(EXTRACT(EPOCH FROM (t.open_time_msk - s.scr_start_time))) NULLS LAST
        ) AS rn
    FROM filtered_scr s
    FULL OUTER JOIN filtered_tmm t
      ON s.symbol = t.symbol
     AND t.open_time_msk BETWEEN s.scr_start_time - INTERVAL '3 hours'
                             AND s.scr_exit_time + INTERVAL '3 hours'
),

-- Финализируем сопоставленные сделки (1 к 1)
final_deals AS (
    SELECT
        symbol,
        scr_start_time,
        scr_exit_time,
        open_time_msk  AS tmm_start_time,
        close_time_msk AS tmm_close_time,
        -- Время сделки: минимальное из источников (устойчивое к NULL)
        LEAST(
            COALESCE(scr_start_time, '9999-01-01'::timestamp),
            COALESCE(open_time_msk, '9999-01-01'::timestamp)
        ) AS min_time
    FROM joined_deals
    WHERE rn = 1
),

-- Агрегация метрик по сделкам из скринера
scr_agg AS (
    SELECT
        d.symbol,
        d.scr_start_time,
        d.scr_exit_time,
        SUM(s.trades_count)::int AS scr_trade_count,
        COUNT(*) * 10.0          AS scr_vol_usd,
        SUM(pnl)                 AS scr_pnl,
        COUNT(*) * 10.0 * 0.001  AS scr_commission,
        COUNT(*) * 10.0 * 0.0006 AS scr_funding,
        MAX(s.maximum_drawdown)  AS scr_drawdown,
        MAX(link)                AS scr_link,
        MAX(s.close_price)       AS scr_close_price
    FROM final_deals d
    LEFT JOIN public.de_1_39_2 s
      ON s.symbol     = d.symbol
     AND s.entry_time >= d.scr_start_time
     AND s.entry_time <= d.scr_exit_time
    GROUP BY d.symbol, d.scr_start_time, d.scr_exit_time
),

-- Универсальная обработка entry_price (число или строковый массив)
scr_entry_prices_agg AS (
    SELECT
        d.symbol,
        d.scr_start_time,
        d.scr_exit_time,
        AVG(x.entry_price)::float AS scr_avg_entry_price,
        MIN(x.entry_price)::float AS scr_first_entry_price
    FROM final_deals d
    JOIN public.de_1_39_2 s
      ON s.symbol     = d.symbol
     AND s.entry_time >= d.scr_start_time
     AND s.entry_time <= d.scr_exit_time
    CROSS JOIN LATERAL (
        SELECT unnest(
            CASE
                WHEN pg_typeof(s.entry_price) = 'text'::regtype
                     AND s.entry_price LIKE '[%' 
                THEN string_to_array(trim(both '[]' from s.entry_price), ',')::float8[]
                ELSE ARRAY[s.entry_price::float8]
            END
        ) AS entry_price
    ) x
    WHERE s.entry_price IS NOT NULL
    GROUP BY d.symbol, d.scr_start_time, d.scr_exit_time
),

-- Агрегация метрик по сделкам из TMM
tmm_agg AS (
    SELECT
        d.symbol,
        d.tmm_start_time,
        d.tmm_close_time,
        SUM(t.qty * t.avg_price_exit) AS tmm_vol_usd,
        SUM(t.realized_pnl)           AS tmm_realized_pnl,
        SUM(t.commission)             AS tmm_commission,
        SUM(t.funding)                AS tmm_funding,
        MAX(t.unit_percent)           AS tmm_unit_percent,
        (MAX(t.duration) / 1000)::int AS tmm_duration_sec,
        SUM(t.trades_count)::int      AS tmm_trade_count,
        MAX(t.maximum_drawdown)       AS tmm_drawdown,
        MAX(t.avg_price_exit)         AS tmm_exit_price,
        MAX(t.avg_price_entry)        AS tmm_avg_entry_price,
        SUM(t.profit_deposit)         AS tmm_profit_deposit,
        -- Первая цена SELL из массива ордеров
        MAX(
            CASE
                WHEN t.orders::jsonb = '[]' THEN NULL
                ELSE (
                    SELECT (elem->>'price')::float
                    FROM jsonb_array_elements(t.orders::jsonb) elem
                    WHERE elem->>'side' = 'SELL'
                      AND elem->>'price' ~ '^[0-9]*\.?[0-9]+$'
                    ORDER BY (elem->>'price')::float
                    LIMIT 1
                )
            END
        ) AS tmm_first_entry_price
    FROM final_deals d
    LEFT JOIN money.tmm_small t
      ON t.symbol = d.symbol
     AND t.open_time + INTERVAL '3 hours' >= d.tmm_start_time
     AND t.open_time + INTERVAL '3 hours' <= d.tmm_close_time
    WHERE t.model_name LIKE '%1233%'
    GROUP BY d.symbol, d.tmm_start_time, d.tmm_close_time
)

-- Финальный отчет: сравнение сделок скринера и TMM
SELECT
    d.symbol,
    d.min_time,
    d.scr_start_time,
    d.tmm_start_time,
    d.scr_exit_time,
    d.tmm_close_time,
    t.tmm_duration_sec,
    s1.scr_first_entry_price,
    t.tmm_first_entry_price,
    s1.scr_avg_entry_price,
    t.tmm_avg_entry_price,
    s.scr_close_price,
    t.tmm_exit_price,
    s.scr_trade_count::int,
    t.tmm_trade_count::int,
    s.scr_vol_usd::numeric,
    t.tmm_vol_usd::numeric,
    s.scr_pnl::numeric,
    t.tmm_unit_percent,
    t.tmm_profit_deposit,
    t.tmm_realized_pnl::numeric,
    s.scr_commission,
    t.tmm_commission,
    s.scr_funding,
    t.tmm_funding,
    s.scr_drawdown,
    t.tmm_drawdown,
    s.scr_link
FROM final_deals d
LEFT JOIN scr_agg s
  ON s.symbol = d.symbol
 AND s.scr_start_time = d.scr_start_time
 AND s.scr_exit_time = d.scr_exit_time
LEFT JOIN tmm_agg t
  ON t.symbol = d.symbol
 AND t.tmm_start_time = d.tmm_start_time
 AND t.tmm_close_time = d.tmm_close_time
LEFT JOIN scr_entry_prices_agg s1
  ON d.symbol = s1.symbol
 AND d.scr_start_time = s1.scr_start_time
 AND d.scr_exit_time = s1.scr_exit_time
ORDER BY d.symbol, d.scr_start_time;
