import requests
import pandas as pd

from config import get_data_from_db

query = """
WITH impressions AS (SELECT
                            imp.id                      AS banner_id,
                            count(imp.user_id)          AS impressions,
                            count(DISTINCT imp.user_id) AS users
                     FROM (SELECT b.device,
                                  b.placement,
                                  b.id,
                                  u.id                                                  as user_id,
                                  trim(split_part(split_part(ue.page, '/', 4), '?', 1)) as page_name
                           FROM user_event ue
                                    INNER JOIN "user" u ON ue.user_id = u.id
                                    LEFT JOIN banner b on ue.attr_1 = b.id::VARCHAR
                           WHERE ue.created::DATE >= current_date - INTERVAL '30 days'
                             AND ue.event = 'banner_show') imp
                     GROUP BY  imp.id),
     imp_24h AS (SELECT
                        imp2.banner_id,
                        count(imp2.user_id) AS impressions_24hours
                 FROM (SELECT b.device,
                              b.placement,
                              b.id                                                  AS banner_id,
                              u.id                                                  AS user_id,
                              trim(split_part(split_part(ue.page, '/', 4), '?', 1)) AS page_name
                       FROM user_event ue
                                INNER JOIN "user" u ON ue.user_id = u.id
                                LEFT JOIN banner b ON ue.attr_1 = b.id::VARCHAR
                       WHERE ue.created::DATE >= current_date - INTERVAL '30 days'
                         AND ue.event = 'banner_show'
                         AND b.id IS NOT NULL
                         AND ue.created::DATE <= u.created::DATE + INTERVAL '24 hours') imp2
                 GROUP BY imp2.banner_id),
     clicks AS (SELECT

                       c2.banner_id                                                  AS banner_id,
                       concat('https://img.fuck-me.io/b/', c2.image_url)             AS banner_url,
                       count(c2.id)                                                  AS clicks,
                       count(c2.id) FILTER (WHERE redirected_to_offer = 'true')      AS redirects,
                       count(c2.id) FILTER (WHERE c2.conversion_type <> 'cpc')       AS cpa_clicks,
                       COALESCE(sum(p.payout), 0) + COALESCE(sum(c2.cpc_revenue), 0) AS revenue,
                       COALESCE(sum(p.payout), 0)                                    AS cpa_revenue
                FROM (SELECT b.device,
                             b.placement,
                             bc.banner_id,
                             b.image_url,
                             b.priority,
                             trim(split_part(split_part(bc.page, '/', 4), '?', 1)) AS page_name,
                             bc.id,
                             bc.redirected_to_offer,
                             o.conversion_type,
                             bc.cpc_revenue,
                             bc.offer_id
                      FROM banner_click bc
                               INNER JOIN "user" u ON bc.user_id = u.id
                               INNER JOIN offer o ON bc.offer_id = o.id
                               INNER JOIN banner b on bc.banner_id = b.id
                      WHERE bc.created::DATE >= current_date - INTERVAL '30 days'
                        AND bc.banner_id IS NOT NULL) AS c2
                         LEFT JOIN postback p ON c2.id = p.banner_click_id
                GROUP BY  c2.banner_id, c2.priority, c2.image_url)
SELECT
       i.banner_id,
       c.banner_url,
       i.users,
       i.impressions,
       i_24.impressions_24hours,
       c.clicks,
       c.redirects,
       c.cpa_clicks,
       c.revenue,
       c.cpa_revenue
FROM impressions i
         LEFT JOIN clicks c
                   ON  i.banner_id = c.banner_id
         LEFT JOIN imp_24h i_24
                   ON  i.banner_id = i_24.banner_id;
"""


def get_gif_size(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return len(response.content) / 1000  # Divide by 1000 to convert bytes to kilobytes
        else:
            print(f"Failed to retrieve the GIF image from {url}")
    except requests.exceptions.RequestException as e:
        print("An error occurred while making the request:", str(e))


df = get_data_from_db(query)

# Create a new column to store the GIF size
df['gif_size'] = None

for index, row in df.iterrows():
    gif_size = get_gif_size(row['banner_url'])
    df.at[index, 'gif_size'] = gif_size
    print(row['banner_url'], gif_size)
print(df.to_csv('banners_weight.csv'))
