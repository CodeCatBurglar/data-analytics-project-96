WITH last_paid_clicks AS (
    -- Выбираем последние платные клики для каждого пользователя
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign,
        ROW_NUMBER() OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC) AS rn
    FROM
        sessions s
    WHERE
        LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
last_clicks AS (
    -- Оставляем только последний платный клик для каждого пользователя
    SELECT
        visitor_id,
        visit_date,
        source,
        medium,
        campaign
    FROM
        last_paid_clicks
    WHERE
        rn = 1
),
lead_data AS (
    -- Собираем данные по лидам, связываем по visitor_id и проверяем, что лид был создан после последнего визита
    SELECT
        lc.visitor_id,
        lc.visit_date,
        lc.source,
        lc.medium,
        lc.campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM
        last_clicks lc
    LEFT JOIN leads l ON lc.visitor_id = l.visitor_id
    AND l.created_at >= lc.visit_date
)
-- Финальный запрос: сортируем по заданным полям
SELECT
    visitor_id,
    visit_date,
    source,
    medium,
    campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
FROM
    lead_data
ORDER BY
    amount DESC NULLS LAST,
    visit_date ASC,
    source,
    medium,
    campaign;