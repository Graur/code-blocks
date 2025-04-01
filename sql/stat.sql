WITH monthly_stats AS (
    SELECT
        date_trunc('month', created) AS month_start,
        COUNT(*) AS row_count,
        pg_total_relation_size('sb_client_event_cloud.client_event') AS table_size_bytes
    FROM sb_client_event_cloud.client_event
    WHERE created BETWEEN '2024-03-01' AND '2025-03-31'
    GROUP BY 1
    ORDER BY 1
)
SELECT
    month_start,
    row_count - LAG(row_count) OVER (ORDER BY month_start) AS record_growth,
    (table_size_bytes - LAG(table_size_bytes) OVER (ORDER BY month_start)) / (1024 * 1024) AS size_growth_mb
FROM monthly_stats;


-------------------------------
WITH monthly_stats AS (
    SELECT
        date_trunc('month', created) AS month_start,
        COUNT(*) AS row_count
    FROM sb_client_event_cloud.client_event
    WHERE created BETWEEN '2024-03-01' AND '2025-03-31'
    GROUP BY 1
),
table_size AS (
    SELECT
        now() AS snapshot_time,
        pg_total_relation_size('sb_client_event_cloud.client_event') AS total_size_bytes
)
SELECT
    m.month_start,
    m.row_count - LAG(m.row_count) OVER (ORDER BY m.month_start) AS record_growth,
    (t.total_size_bytes - LAG(t.total_size_bytes) OVER (ORDER BY m.month_start)) / (1024 * 1024) AS size_growth_mb
FROM monthly_stats m
JOIN table_size t ON t.snapshot_time >= m.month_start
ORDER BY m.month_start;

-----------------------------------------

SELECT
    date_trunc('month', now()) AS month_start,
    relname,
    n_tup_ins - LAG(n_tup_ins) OVER (ORDER BY date_trunc('month', now())) AS record_growth,
    (pg_total_relation_size(relname) - LAG(pg_total_relation_size(relname)) OVER (ORDER BY date_trunc('month', now()))) / (1024 * 1024) AS size_growth_mb
FROM pg_stat_user_tables
WHERE relname = 'client_event'
ORDER BY month_start;

-------------------------------------------------------------


WITH record_size AS (
    -- Вычисляем средний размер одной строки по 1000 записям
    SELECT AVG(pg_column_size(t)) AS avg_row_size
    FROM (SELECT * FROM sb_client_event_cloud.client_event LIMIT 1000) t
),
monthly_stats AS (
    -- Считаем количество новых записей по месяцам
    SELECT
        date_trunc('month', created) AS month_start,
        COUNT(*) AS record_growth
    FROM sb_client_event_cloud.client_event
    WHERE created BETWEEN '2024-03-01' AND '2025-03-31'
    GROUP BY 1
    ORDER BY 1
)
SELECT
    m.month_start,
    m.record_growth,
    (m.record_growth * r.avg_row_size) / (1024 * 1024) AS size_growth_mb
FROM monthly_stats m
JOIN record_size r ON true;

--------------------------------------------------------
