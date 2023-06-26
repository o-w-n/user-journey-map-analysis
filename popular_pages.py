from collections import Counter
import pandas as pd
from config import get_data_from_db
import json

query = """
            SELECT u.user_id,
                   MAX(u.conversion_funnel_id) AS funnel_id,
                   ARRAY_AGG(u.page_name ORDER BY u.created) AS journey_pages
            FROM (
                SELECT ue.user_id,
                       ue.created,
                       conversion_funnel_id,
                       ue.event,
                       ue.page,
         CASE
                 WHEN ue.event = 'route' or ue.event = 'user_visit' and LENGTH(ue.page) > 19 AND LENGTH(ue.page) < 50 THEN
                     CASE
                         WHEN regexp_count(ue.page, '/') = 3 THEN
                             CASE
                                 WHEN ue.page NOT LIKE '%id=%' THEN COALESCE(
                                         trim(split_part(split_part(ue.page, '/', 4), '?', 1)), '')
                                 ELSE trim(split_part(split_part(ue.page, '/', 4), '?', 1))
                                 END
                         WHEN regexp_count(ue.page, '/') = 4 THEN
                             CASE
                                 WHEN split_part(ue.page, '/', 4) LIKE '%profile%' or
                                      split_part(ue.page, '/', 4) LIKE '%matches%' THEN COALESCE(
                                         trim(split_part(split_part(ue.page, '/', 4), '?', 1)), '')
                                 ELSE trim(split_part(split_part(ue.page, '/', 4), '?', 1))
                                 END
                         END
                 END                                                                         AS page_name
                FROM user_event ue
                INNER JOIN "user" u ON u.id = ue.user_id
                WHERE ue.created::date >= current_date - interval '14 day'
                ORDER BY ue.user_id, ue.created
            ) u
            WHERE u.page_name IS NOT NULL
            and  MAX(u.conversion_funnel_id) = 1
            GROUP BY 1
        """

df = pd.read_csv('user_anal_db.csv')


def analyze_userflow(data):



    data['journey_pages'] = data['journey_pages'].apply(lambda x: str(x) if pd.notnull(x) else 'Null')
    data['journey_pages'] = data['journey_pages'].str.split(',')
    filtered_df = data.query(
        "product_time > 9 and banner_clicks == 0 and attr_clicks == 0 and redirected == 0 "
        "and session_number <= 5"
    )
    all_interactions = [interactions for journey_pages in filtered_df['journey_pages']
                        for interactions in journey_pages]

    # browse_pages_count = filtered_df['journey_pages'].apply(lambda x: x.count('browse')).sum()
    # total_users = filtered_df['user_id'].nunique()
    # average_browse_pages = browse_pages_count / total_users
    # print(average_browse_pages)
    popular_interactions = Counter(all_interactions).most_common()
    return popular_interactions


popular = analyze_userflow(df)

print("\nThe most popular interactions:")
for interaction, count in popular:
    print(f"{interaction},{count}")
