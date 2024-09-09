WITH last_paid_clicks AS (
    -- Определяем последние платные клики для каждого пользователя
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
ad_spend AS (
    -- Собираем данные по затратам на рекламу из таблиц ya_ads и vk_ads
    SELECT
        campaign_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM
        ya_ads
    GROUP BY
        campaign_date, utm_source, utm_medium, utm_campaign
    UNION ALL
    SELECT
        campaign_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM
        vk_ads
    GROUP BY
        campaign_date, utm_source, utm_medium, utm_campaign
),
leads_data AS (
    -- Собираем данные по лидам, связываем с последними кликами
    SELECT
        lc.visit_date,
        lc.source,
        lc.medium,
        lc.campaign,
        COUNT(l.lead_id) AS leads_count,
        COUNT(CASE WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 THEN 1 END) AS purchases_count,
        SUM(CASE WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 THEN l.amount ELSE 0 END) AS revenue
    FROM
        last_clicks lc
    LEFT JOIN leads l ON lc.visitor_id = l.visitor_id AND l.created_at >= lc.visit_date
    GROUP BY
        lc.visit_date, lc.source, lc.medium, lc.campaign
),
final_data AS (
    -- Объединяем все данные, включая количество визитов, затраты и лиды
    SELECT
        lc.visit_date,
        lc.source,
        lc.medium,
        lc.campaign,
        COUNT(lc.visitor_id) AS visitors_count,
        COALESCE(a.total_cost, 0) AS total_cost,
        COALESCE(ld.leads_count, 0) AS leads_count,
        COALESCE(ld.purchases_count, 0) AS purchases_count,
        COALESCE(ld.revenue, 0) AS revenue
    FROM
        last_clicks lc
    LEFT JOIN ad_spend a ON lc.visit_date = a.visit_date AND lc.source = a.utm_source AND lc.medium = a.utm_medium AND lc.campaign = a.utm_campaign
    LEFT JOIN leads_data ld ON lc.visit_date = ld.visit_date AND lc.source = ld.source AND lc.medium = ld.medium AND lc.campaign = ld.campaign
    GROUP BY
        lc.visit_date, lc.source, lc.medium, lc.campaign, a.total_cost, ld.leads_count, ld.purchases_count, ld.revenue
)
-- Финальный запрос с сортировкой по заданным условиям
SELECT
    visit_date,
    source,
    medium,
    campaign,
    visitors_count,
    total_cost,
    leads_count,
    purchases_count,
    revenue
FROM
    final_data
order BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    source,
    medium,
    campaign
limit 15;