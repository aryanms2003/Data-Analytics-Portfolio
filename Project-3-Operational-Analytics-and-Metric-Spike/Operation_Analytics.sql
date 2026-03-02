Operation Analytics and Investigating Metric Spike

1.1)
SELECT 
    ds AS DATE,
    COUNT(job_id) AS job_count,
    ROUND(SUM(time_spent) / 3600, 2) AS total_time_spent_hours,
    ROUND(COUNT(job_id) / (SUM(time_spent) / 3600), 2) AS review_per_hour
FROM project3_trainity.job_data
WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY ds
ORDER BY ds;

1.2)
SELECT 
    ds AS DATE,
    ROUND(COUNT(event) / SUM(time_spent), 2) AS daily_rolling_average
FROM project3_trainity.job_data
GROUP BY ds
ORDER BY ds;

1.3)
SELECT 
    language,
    ROUND(100 * COUNT(*) / job_total.total, 2) AS percentage
FROM project3_trainity.job_data
CROSS JOIN (
    SELECT COUNT(*) AS total 
    FROM project3_trainity.job_data
) AS job_total
GROUP BY language, job_total.total;

1.4)
SELECT 
    actor_id,
    COUNT(*) AS duplicate_count
FROM project3_trainity.job_data
GROUP BY actor_id
HAVING COUNT(*) > 1;

2.1)
SELECT 
    EXTRACT(WEEK FROM occurred_at) AS week,
    COUNT(DISTINCT user_id) AS active_users
FROM project3_trainity.events
WHERE event_type = 'engagement'
GROUP BY week
ORDER BY week;


2.2)
WITH active_users_per_week AS (
    SELECT 
        EXTRACT(YEAR FROM created_at) AS year,
        EXTRACT(WEEK FROM created_at) AS week,
        COUNT(DISTINCT user_id) AS total_users
    FROM project3_trainity.users
    GROUP BY year, week
)
SELECT 
    year,
    week,
    total_users,
    SUM(total_users) OVER (ORDER BY year, week) AS cumulative_growth
FROM active_users_per_week
ORDER BY year, week;

2.3)
SELECT 
    first AS cohort_week,

    SUM(CASE WHEN week_num = 0 THEN 1 ELSE 0 END) AS week_0,
    SUM(CASE WHEN week_num = 1 THEN 1 ELSE 0 END) AS week_1,
    SUM(CASE WHEN week_num = 2 THEN 1 ELSE 0 END) AS week_2,
    SUM(CASE WHEN week_num = 3 THEN 1 ELSE 0 END) AS week_3,
    SUM(CASE WHEN week_num = 4 THEN 1 ELSE 0 END) AS week_4,
    SUM(CASE WHEN week_num = 5 THEN 1 ELSE 0 END) AS week_5,
    SUM(CASE WHEN week_num = 6 THEN 1 ELSE 0 END) AS week_6,
    SUM(CASE WHEN week_num = 7 THEN 1 ELSE 0 END) AS week_7,
    SUM(CASE WHEN week_num = 8 THEN 1 ELSE 0 END) AS week_8,
    SUM(CASE WHEN week_num = 9 THEN 1 ELSE 0 END) AS week_9,
    SUM(CASE WHEN week_num = 10 THEN 1 ELSE 0 END) AS week_10,
    SUM(CASE WHEN week_num = 11 THEN 1 ELSE 0 END) AS week_11,
    SUM(CASE WHEN week_num = 12 THEN 1 ELSE 0 END) AS week_12,
    SUM(CASE WHEN week_num = 13 THEN 1 ELSE 0 END) AS week_13,
    SUM(CASE WHEN week_num = 14 THEN 1 ELSE 0 END) AS week_14,
    SUM(CASE WHEN week_num = 15 THEN 1 ELSE 0 END) AS week_15,
    SUM(CASE WHEN week_num = 16 THEN 1 ELSE 0 END) AS week_16,
    SUM(CASE WHEN week_num = 17 THEN 1 ELSE 0 END) AS week_17,
    SUM(CASE WHEN week_num = 18 THEN 1 ELSE 0 END) AS week_18

FROM (
    SELECT 
        l.user_id,
        l.login_week,
        f.first,
        l.login_week - f.first AS week_num
    FROM (
        SELECT 
            user_id,
            EXTRACT(WEEK FROM occurred_at) AS login_week
        FROM project3_trainity.events
        GROUP BY user_id, login_week
    ) l
    JOIN (
        SELECT 
            user_id,
            MIN(EXTRACT(WEEK FROM occurred_at)) AS first
        FROM project3_trainity.events
        GROUP BY user_id
    ) f
    ON l.user_id = f.user_id
) sub
GROUP BY first
ORDER BY first;

2.4)
SELECT 
    EXTRACT(WEEK FROM occurred_at) AS week,

    COUNT(DISTINCT CASE WHEN device = 'acer aspire desktop' THEN user_id END) AS acer_aspire_desktop,
    COUNT(DISTINCT CASE WHEN device = 'acer aspire notebook' THEN user_id END) AS acer_aspire_notebook,
    COUNT(DISTINCT CASE WHEN device = 'amazon fire phone' THEN user_id END) AS amazon_fire_phone,
    COUNT(DISTINCT CASE WHEN device = 'asus chromebook' THEN user_id END) AS asus_chromebook,
    COUNT(DISTINCT CASE WHEN device = 'dell inspiron notebook' THEN user_id END) AS dell_inspiron_notebook,
    COUNT(DISTINCT CASE WHEN device = 'dell inspiron desktop' THEN user_id END) AS dell_inspiron_desktop,
    COUNT(DISTINCT CASE WHEN device = 'hp pavilion desktop' THEN user_id END) AS hp_pavilion_desktop,
    COUNT(DISTINCT CASE WHEN device = 'htc one' THEN user_id END) AS htc_one,
    COUNT(DISTINCT CASE WHEN device = 'ipad air' THEN user_id END) AS ipad_air,
    COUNT(DISTINCT CASE WHEN device = 'ipad mini' THEN user_id END) AS ipad_mini,
    COUNT(DISTINCT CASE WHEN device = 'iphone 4s' THEN user_id END) AS iphone_4s,
    COUNT(DISTINCT CASE WHEN device = 'iphone 5' THEN user_id END) AS iphone_5,
    COUNT(DISTINCT CASE WHEN device = 'iphone 5s' THEN user_id END) AS iphone_5s,
    COUNT(DISTINCT CASE WHEN device = 'kindle fire' THEN user_id END) AS kindle_fire,
    COUNT(DISTINCT CASE WHEN device = 'lenovo thinkpad' THEN user_id END) AS lenovo_thinkpad,
    COUNT(DISTINCT CASE WHEN device = 'macbook air' THEN user_id END) AS macbook_air,
    COUNT(DISTINCT CASE WHEN device = 'macbook pro' THEN user_id END) AS macbook_pro,
    COUNT(DISTINCT CASE WHEN device = 'nexus 10' THEN user_id END) AS nexus_10,
    COUNT(DISTINCT CASE WHEN device = 'nexus 7' THEN user_id END) AS nexus_7,
    COUNT(DISTINCT CASE WHEN device = 'nexus 5' THEN user_id END) AS nexus_5,
    COUNT(DISTINCT CASE WHEN device = 'nokia lumia 635' THEN user_id END) AS nokia_lumia_635,
    COUNT(DISTINCT CASE WHEN device = 'samsung galaxy tablet' THEN user_id END) AS samsung_galaxy_tablet,
    COUNT(DISTINCT CASE WHEN device = 'samsung galaxy note' THEN user_id END) AS samsung_galaxy_note,
    COUNT(DISTINCT CASE WHEN device = 'samsung galaxy s4' THEN user_id END) AS samsung_galaxy_s4,
    COUNT(DISTINCT CASE WHEN device = 'windows surface' THEN user_id END) AS windows_surface

FROM project3_trainity.events
WHERE event_type = 'engagement'
GROUP BY week
ORDER BY week;

2.5)
SELECT *,
    CASE 
        WHEN action IN ('sent_weekly_digest', 'sent_reengagement_email') 
            THEN 'email_sent'
        WHEN action = 'email_open' 
            THEN 'email_open'
        WHEN action = 'email_clickthrough' 
            THEN 'email_click'
        ELSE NULL
    END AS email_action
FROM project3_trainity.emailevents;

2.6)
SELECT 
    100.0 * SUM(CASE WHEN email_action = 'email_open' THEN 1 ELSE 0 END) 
        / SUM(CASE WHEN email_action = 'email_sent' THEN 1 ELSE 0 END) 
        AS open_rate,

    100.0 * SUM(CASE WHEN email_action = 'email_click' THEN 1 ELSE 0 END) 
        / SUM(CASE WHEN email_action = 'email_sent' THEN 1 ELSE 0 END) 
        AS click_rate

FROM (
    SELECT *,
        CASE 
            WHEN action IN ('sent_weekly_digest', 'sent_reengagement_email') 
                THEN 'email_sent'
            WHEN action = 'email_open' 
                THEN 'email_open'
            WHEN action = 'email_clickthrough' 
                THEN 'email_click'
            ELSE NULL
        END AS email_action
    FROM project3_trainity.emailevents
) a;