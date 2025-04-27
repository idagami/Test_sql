-- task 1
WITH fg_ads AS (
    SELECT
        ad_date,
        url_parameters,
        COALESCE(f.spend, 0) AS spend,
        COALESCE(f.impressions, 0) AS impressions,
        COALESCE(f.reach, 0) AS reach,
        COALESCE(f.clicks, 0) AS clicks,
        COALESCE(f.leads, 0) AS leads,
        COALESCE(f.value, 0) AS value
    FROM facebook_ads_basic_daily f
    UNION ALL
    SELECT
        ad_date,
        url_parameters,
        COALESCE(g.spend, 0) AS spend,
        COALESCE(g.impressions, 0) AS impressions,
        COALESCE(g.reach, 0) AS reach,
        COALESCE(g.clicks, 0) AS clicks,
        COALESCE(g.leads, 0) AS leads,
        COALESCE(g.value, 0) AS value
    FROM google_ads_basic_daily g
),
final_data AS (
    SELECT
        date(date_trunc('month', fg_ads.ad_date)) AS ad_month,
        CASE
            WHEN id_decode_url(LOWER(SUBSTRING(fg_ads.url_parameters, 'utm_campaign=([^$#&]+)'))) = 'nan' THEN NULL
            ELSE id_decode_url(LOWER(SUBSTRING(fg_ads.url_parameters, 'utm_campaign=([^$#&]+)')))
        END AS utm_campaign,
        SUM(fg_ads.spend) AS t_spend,
        SUM(fg_ads.impressions) AS t_impressions,
        SUM(fg_ads.clicks) AS t_clicks,
        SUM(fg_ads.value) AS t_value,
        CASE 
            WHEN SUM(fg_ads.impressions) = 0 THEN 0
            ELSE ROUND((SUM(fg_ads.clicks)::numeric / SUM(fg_ads.impressions)) * 100,2)
        END AS CTR,
        CASE
            WHEN SUM(fg_ads.clicks) = 0 THEN 0
            ELSE ROUND(SUM(fg_ads.spend)::numeric / SUM(fg_ads.clicks),2) 
        END AS CPC,
        CASE
            WHEN SUM(fg_ads.impressions) = 0 THEN 0
            ELSE ROUND((SUM(fg_ads.spend) * 1000)::numeric / SUM(fg_ads.impressions),2) 
        END AS CPM,
        CASE
            WHEN SUM(fg_ads.value) = 0 THEN 0
            ELSE ROUND((SUM(fg_ads.value) - SUM(fg_ads.spend))::numeric / SUM(fg_ads.value) * 100,2) 
        END AS ROMI
    FROM fg_ads
    GROUP BY
        ad_month,
        utm_campaign
)
SELECT
    ad_month,
    utm_campaign,
    t_spend,
    t_impressions,
    t_clicks,
    t_value,
    CONCAT(CTR, '%') AS CTR,
    CPC,
    CPM,
    CONCAT(ROMI, '%') AS ROMI,
    COALESCE(CPM - LAG(CPM, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month), 0) AS CPM_diff,
    CONCAT(
        COALESCE(CTR - LAG(CTR, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month), 0), '%'
    ) AS CTR_diff,
    CONCAT(
        COALESCE(ROMI - LAG(ROMI, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month), 0), '%'
    ) AS ROMI_diff
FROM final_data
WHERE utm_campaign IS NOT NULL
ORDER BY ad_month;