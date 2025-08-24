-- Задача: анализ исторических сделок и построение прогнозных траекторий PnL.
-- Скрипт строит фактические и прогнозные линии доходности на основе статистики по сделкам,
-- включая регрессию, медианные линии и перцентили распределений прибыли.

WITH filtered AS (
    -- Отбор сделок по нужной модели, исключая вручную закрытые и помеченные как "баг"
    SELECT *
    FROM money.tmm_small
    WHERE model_name    LIKE '%1232%'
      AND close_time    > '2025-04-03'
      AND category_name NOT IN ('ЗАКРЫТА РУКАМИ', 'БАГ')
),
last_close_time AS (
    -- Последняя фактическая точка: время закрытия и кумулятивный PnL на ней
    SELECT 
        close_time AS max_time,
        cum_pnl    AS last_cum_pnl
    FROM (
        SELECT 
            close_time,
            SUM(net_profit * 10) OVER (ORDER BY close_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_pnl,
            ROW_NUMBER()        OVER (ORDER BY close_time DESC, id DESC) AS rn
        FROM filtered
    ) sub
    WHERE rn = 1
),
t_stat AS (
    -- Базовая статистика распределения прибыли сделок
    SELECT
        COUNT(*)                                                            AS n,
        STDDEV(net_profit)                                                  AS stddev,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY net_profit)             AS median,
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY net_profit)            AS p05,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY net_profit)            AS p95
    FROM filtered
),
avg_trades_per_day AS (
    -- Среднее число сделок в день (с защитой от деления на ноль)
    SELECT 
        COUNT(*) / NULLIF(EXTRACT(EPOCH FROM DATE_TRUNC('day', MAX(close_time)) 
                                - DATE_TRUNC('day', MIN(close_time))) / 86400.0, 0.0) AS avg_trades_per_day
    FROM filtered
),
student_coeff AS (
    -- Нормированные смещения перцентилей на последних 300 сделках
    WITH base_data AS (
        SELECT net_profit
        FROM filtered
        ORDER BY close_time DESC, id DESC
        LIMIT 300
    ),
    stats AS (
        SELECT
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY net_profit) AS median,
            STDDEV(net_profit)                                      AS stddev
        FROM base_data
    ),
    percentiles AS (
        SELECT
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY net_profit) AS p01,
            PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY net_profit) AS p05,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY net_profit) AS p25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY net_profit) AS p75,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY net_profit) AS p95,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY net_profit) AS p99
        FROM base_data
    )
    SELECT
        (p01 - median) / NULLIF(stddev, 0) AS p01,
        (p05 - median) / NULLIF(stddev, 0) AS p05,
        (p25 - median) / NULLIF(stddev, 0) AS p25,
        (p75 - median) / NULLIF(stddev, 0) AS p75,
        (p95 - median) / NULLIF(stddev, 0) AS p95,
        (p99 - median) / NULLIF(stddev, 0) AS p99
    FROM percentiles, stats
),
ranked AS (
    -- Кумулятивный PnL по сделкам (без "predict"), с порядковым номером сделки
    SELECT
        close_time,
        SUM(net_profit * 10) OVER (ORDER BY close_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_pnl,
        ROW_NUMBER()        OVER (ORDER BY close_time, id) AS t
    FROM filtered
    WHERE category_name <> 'predict'
),
first_last AS (
    -- Первая и последняя точка ряда + медиана по кумулятивному PnL
    SELECT
        MAX(t)                                                      AS max_t,
        MIN(t)                                                      AS min_t,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cum_pnl)        AS median_start,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cum_pnl) FILTER (WHERE t = (SELECT MIN(t) FROM ranked)) AS first,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cum_pnl) FILTER (WHERE t = (SELECT MAX(t) FROM ranked)) AS last
    FROM ranked
),
slope AS (
    -- Наклон медианной линии кумулятивного PnL
    SELECT (last - first) / NULLIF(max_t - min_t, 0) AS k, first
    FROM first_last
),
regression_source AS (
    -- Данные для линейной регрессии PnL по времени
    SELECT
        close_time,
        ROW_NUMBER()        OVER (ORDER BY close_time, id) AS t,
        SUM(net_profit * 10) OVER (ORDER BY close_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_pnl
    FROM filtered
    WHERE category_name <> 'predict'
),
regression_stats AS (
    -- Промежуточные суммы для формулы коэффициента регрессии
    SELECT
        COUNT(*)        AS n,
        SUM(t * t)      AS sum_tt,
        SUM(t * cum_pnl) AS sum_ty,
        SUM(t)          AS sum_t,
        SUM(cum_pnl)    AS sum_y
    FROM regression_source
),
regression_coeff AS (
    -- Коэффициент наклона регрессии
    SELECT
        (n * sum_ty - sum_t * sum_y) / NULLIF(n * sum_tt - sum_t * sum_t, 0) AS b
    FROM regression_stats
),
regression_median_line AS (
    -- Линия "медианного регрессионного прироста"
    SELECT
        r.close_time,
        r.t * t.median * 10 AS cum_median_regression
    FROM (
        SELECT 
            close_time,
            ROW_NUMBER() OVER (ORDER BY close_time, id) AS t
        FROM filtered
        WHERE category_name <> 'predict'
    ) r
    CROSS JOIN (
        SELECT 
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY net_profit) AS median
        FROM filtered
    ) t
),
regression_line AS (
    -- Основная регрессионная линия PnL
    SELECT
        r.close_time,
        r.t * rc.b AS cum_regression
    FROM regression_source r
    CROSS JOIN regression_coeff rc
),
median_cum_pnl_line AS (
    -- Линия медианного кумулятивного PnL
    SELECT
        r.close_time,
        r.t * s.k + s.first AS cum_median_cum_pnl
    FROM ranked r
    CROSS JOIN slope s
),
actual_cum AS (
    -- Фактические кумулятивные траектории (PnL, регрессии, медианы)
    SELECT
        a.close_time,
        a.cum_pnl::double precision,
        NULL::double precision AS cum_p01,
        NULL::double precision AS cum_p25,
        NULL::double precision AS cum_p75,
        NULL::double precision AS cum_p95,
        NULL::double precision AS cum_p05,
        NULL::double precision AS cum_p99,
        r.cum_regression::double precision,
        m.cum_median_regression::double precision,
        c.cum_median_cum_pnl::double precision,
        'real' AS label
    FROM (
        SELECT
            close_time,
            SUM(net_profit * 10) OVER (ORDER BY close_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_pnl
        FROM filtered
    ) a
    LEFT JOIN regression_line        r ON a.close_time = r.close_time
    LEFT JOIN regression_median_line m ON a.close_time = m.close_time
    LEFT JOIN median_cum_pnl_line    c ON a.close_time = c.close_time
),
last_median_cum AS (
    -- Последнее значение медианной линии (для продолжения прогноза)
    SELECT c.cum_median_cum_pnl AS last
    FROM actual_cum c
    ORDER BY c.close_time DESC
    LIMIT 1
),
forecast_points AS (
    -- Прогнозные точки (cum_pnl и перцентили) на основе статистики
    -- ВАЖНО: шаг и количество шагов считаются через LATERAL + COALESCE,
    -- чтобы generate_series и деление интервала не получили NULL.
    SELECT
        l.max_time + gs.step * pace.step_interval                                                               AS close_time,
        (l.last_cum_pnl + (gs.step - 1) * t_stat.median * 10)                                                   ::double precision AS cum_pnl,
        (l.last_cum_pnl + (gs.step - 1) * (t_stat.median + student_coeff.p01 * t_stat.stddev) * 10)             ::double precision AS cum_p01,
        (l.last_cum_pnl + (gs.step - 1) * (t_stat.median + student_coeff.p25 * t_stat.stddev) * 10)             ::double precision AS cum_p25,
        (l.last_cum_pnl + (gs.step - 1) * (t_stat.median + student_coeff.p75 * t_stat.stddev) * 10)             ::double precision AS cum_p75,
        (l.last_cum_pnl + (gs.step - 1) * (t_stat.median + student_coeff.p95 * t_stat.stddev) * 10)             ::double precision AS cum_p95,
        (l.last_cum_pnl + (gs.step - 1) * (t_stat.median + student_coeff.p05 * t_stat.stddev) * 10)             ::double precision AS cum_p05,
        (l.last_cum_pnl + (gs.step - 1) * (t_stat.median + student_coeff.p99 * t_stat.stddev) * 10)             ::double precision AS cum_p99,
        NULL::double precision                                                                                   AS cum_regression,
        NULL::double precision                                                                                   AS cum_median_regression,
        (last_median_cum.last + gs.step * s.k)                                                                   ::double precision AS cum_median_cum_pnl,
        'predicted'                                                                                              AS label
    FROM t_stat
    CROSS JOIN last_close_time l
    CROSS JOIN student_coeff
    CROSS JOIN slope s
    CROSS JOIN last_median_cum
    -- Конфигурация шага/кол-ва точек прогноза (не даём NULL попасть в вычисления)
    CROSS JOIN LATERAL (
        SELECT
            COALESCE(GREATEST(1, ROUND(avg_trades_per_day * 3)::int), 1)                                         AS steps,
            (interval '3 days') / COALESCE(GREATEST(1, ROUND(avg_trades_per_day * 3)::int), 1)::double precision AS step_interval
        FROM avg_trades_per_day
    ) AS pace
    CROSS JOIN LATERAL generate_series(1, pace.steps) AS gs(step)
),
combined AS (
    -- Объединение фактических и прогнозных траекторий
    SELECT * FROM actual_cum
    UNION ALL
    SELECT * FROM forecast_points
),
final_with_fields AS (
    -- Добавление полей сделок к результату
    SELECT
        c.*,
        t.id,
        t.symbol,
        t.open_time,
        t.duration,
        t.commission,
        t.realized_pnl,
        t.percent,
        t.qty,
        t.avg_price_entry,
        t.avg_price_exit,
        t.net_profit,
        t.max_win_percent,
        t.max_loose_percent,
        t.peak_qty,
        t.profit_deposit,
        t.funding,
        t.volume,
        t.closed_value,
        t.open_qty,
        t.orders,
        t.rounded_qty,
        t.trades_count,
        t.unit_percent,
        t.model_name,
        t.maximum_drawdown,
        CASE WHEN label = 'predicted' THEN 'predict' ELSE t.category_name END AS category_name
    FROM combined c
    LEFT JOIN filtered t
      ON c.close_time = t.close_time
)
-- Финальный результат: фактические + прогнозные ряды с деталями сделок
SELECT *
FROM final_with_fields
ORDER BY close_time;

