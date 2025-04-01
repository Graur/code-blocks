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
