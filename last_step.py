import json

import pandas as pd
from collections import Counter

from config import get_data_from_db

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 3000)
pd.set_option('display.max_rows', 2000)
# Define the data as a list of dictionaries


query_user_journey = """
SELECT base.user_id,
       base.session_number,
       max(pt.product_time)                                                                       as product_time,
       max(base.offer_id)                                                                         as offer_id,
       max(o.offer_category_id)                                                                   as offer_category_id,
       max(base.os)                                                                               as os,
       max(base.device_vendor)                                                                    as device_vendor,
       MAX(base.conversion_funnel_id)                                                             AS funnel_id,
       concat(ROUND(MAX((base.user_info ->> 'screen_size_width')::int) / 10) * 10, 'x',
              ROUND(MAX((base.user_info ->> 'screen_size_height')::int) / 10) * 10)               as screen_size,
       CASE
           WHEN REPLACE(ARRAY_TO_STRING(ARRAY_AGG(base.page_name ORDER BY base.created), ','), 'null', '') = '' THEN 0
           WHEN REPLACE(ARRAY_TO_STRING(ARRAY_AGG(base.page_name ORDER BY base.created), ','), 'null', '') <> '' THEN
                   regexp_count(
                           REPLACE(ARRAY_TO_STRING(ARRAY_AGG(base.page_name ORDER BY base.created), ','), 'null', ''),
                           ','
                       ) + 1
           ELSE 1 END                                                                             as journey_len,
       REPLACE(ARRAY_TO_STRING(ARRAY_AGG(base.page_name ORDER BY base.created), ','), 'null', '') AS journey_pages,
       count(*) FILTER (WHERE base.event = 'bnr_click' AND base.banner_id ~ '^([0-9]+$)')         as banner_clicks,
       count(*) FILTER (WHERE base.event = 'bnr_click' AND base.banner_id ~ '[A-Za-z]')           as attr_clicks,
       count(*) FILTER (WHERE base.event = 'redirected_to_offer')                                 as redirected

FROM (SELECT ue.user_id,
             u.os,
             u.device_vendor,
             u.user_info,
             ue.created,
             conversion_funnel_id,
             ue.event,
             ue.page,
             uv.session_start,
             LEAD(uv.session_start) OVER (PARTITION BY uv.user_id ORDER BY uv.session_start) AS session_end,
             dense_rank() OVER (PARTITION BY uv.user_id ORDER BY uv.session_start)           AS session_number,
             CASE
                 WHEN ue.event = 'route' and LENGTH(ue.page) > 19 AND LENGTH(ue.page) < 50
                     THEN
                     CASE
                         WHEN regexp_count(ue.page, '/') = 3 THEN
                             CASE
                                 WHEN ue.page NOT LIKE '%id=%' THEN COALESCE(
                                         trim(split_part(split_part(ue.page, '/', 4), '?', 1)), '')
                                 ELSE
                                     CONCAT(
                                             COALESCE(trim(split_part(split_part(ue.page, '/', 4), '?', 1)), ''),
                                             '[',
                                             COALESCE(trim(split_part(split_part(ue.page, '/', 4), 'id=', 2)), ''),
                                             ']'
                                         )
                                 END
                         WHEN regexp_count(ue.page, '/') = 4 THEN
                             CASE
                                 WHEN split_part(ue.page, '/', 4) NOT LIKE '%profile%' AND
                                      split_part(ue.page, '/', 4) NOT LIKE '%matches%' THEN COALESCE(
                                         trim(split_part(split_part(ue.page, '/', 4), '?', 1)), '')
                                 ELSE CONCAT(
                                         COALESCE(trim(split_part(ue.page, '/', 4)), ''),
                                         '[',
                                         COALESCE(trim(split_part(ue.page, '/', 5)), ''),
                                         ']'
                                     )
                                 END
                         END
                 END                                                                         AS page_name,


             CASE
                 WHEN ue.event = 'bnr_click' and ue.attr_1 <> 'null' THEN ue.attr_1
                 ELSE attr_2
                 END                                                                         AS banner_id,
             CAST(CASE WHEN ue.event = 'redirected_to_offer' THEN ue.attr_1 END AS INTEGER)  AS offer_id
      FROM user_event ue
               INNER JOIN "user" u ON u.id = ue.user_id
               INNER JOIN user_visit uv on uv.id = ue.user_visit_id::uuid
      WHERE ue.created::date >= current_date - interval '14 day'
        and uv.session_start::date >= current_date - interval '14 day'
      ORDER BY ue.user_id, ue.created) base
         left join offer o ON base.offer_id::integer = o.id
         left join (select user_id, sum(time_difference) as product_time
                    from (select user_id,
                                 EXTRACT(EPOCH FROM (ue.created - LAG(ue.created, 1) OVER
                                     (PARTITION BY user_id ORDER BY ue.created)))
                                     AS time_difference
                          from "user" u
                                   left join user_event ue on u.id = ue.user_id
                          WHERE ue.created::date >= current_date - interval '14 day'
                            and ue.created <= u.created + interval '24 hours'
                            and u.device = 'mobile'
                            and ue.event not in ('user_afk', 'user_socket_disconnect')) time_diff
                    where time_difference <= 900
                    group by 1) pt on pt.user_id = base.user_id
where base.session_number <= 5
GROUP BY base.user_id, base.session_number;                                                           
"""

query_user_journey_after_chat_message_limit_reached = """
/*routes users after chat_message_limit_reached*/
with base as (SELECT ue.user_id,
                     ue.event,
                     ue.created,
                     u.conversion_funnel_id,
                     dense_rank() OVER (PARTITION BY uv.user_id ORDER BY uv.session_start) AS session_number,
                     ue.created                                                            as event_created,
                     CASE
                         WHEN event = 'route' and LENGTH(ue.page) > 19 AND LENGTH(ue.page) < 50
                             THEN
                             CASE
                                 WHEN regexp_count(ue.page, '/') = 3 THEN
                                     CASE
                                         WHEN ue.page NOT LIKE '%id=%' THEN COALESCE(
                                                 trim(split_part(split_part(ue.page, '/', 4), '?', 1)), '')
                                         ELSE
                                             CONCAT(
                                                     COALESCE(trim(split_part(split_part(ue.page, '/', 4), '?', 1)),
                                                              ''),
                                                     '[',
                                                     COALESCE(trim(split_part(split_part(ue.page, '/', 4), 'id=', 2)),
                                                              ''),
                                                     ']'
                                                 )
                                         END
                                 WHEN regexp_count(ue.page, '/') = 4 THEN
                                     CASE
                                         WHEN split_part(ue.page, '/', 4) NOT LIKE '%profile%' AND
                                              split_part(ue.page, '/', 4) NOT LIKE '%matches%' THEN COALESCE(
                                                 trim(split_part(split_part(ue.page, '/', 4), '?', 1)), '')
                                         ELSE CONCAT(
                                                 COALESCE(trim(split_part(ue.page, '/', 4)), ''),
                                                 '[',
                                                 COALESCE(trim(split_part(ue.page, '/', 5)), ''),
                                                 ']'
                                             )
                                         END
                                 END
                         END                                                               AS page_name
              FROM user_event ue
                       INNER JOIN user_visit uv on uv.id = ue.user_visit_id::uuid
                       INNER JOIN "user" u on u.id = ue.user_id
              WHERE ue.created::date >= current_date - interval '7 days'),
     base1 as (select user_id, session_number, MAX(conversion_funnel_id), min(event_created) as chat_limit_time
               from base
               where event = 'chat_message_limit_reached'
               group by 1, 2)
select base.user_id,
       base.session_number,
       max(base.conversion_funnel_id),
       REPLACE(ARRAY_TO_STRING(ARRAY_AGG(base.page_name ORDER BY base.created), ','), 'null', '') AS journey_pages
from base
         left join base1 on base.user_id = base1.user_id and base.session_number = base1.session_number
where base.event = 'route'
  and base.event_created > base1.chat_limit_time
    and base.session_number <=5
group by 1, 2
order by 1, 2;
"""


def save_json(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file)


def general_pages(data):
    return [page.split('[')[0] if '[' in page else page for page in str(data).split(',')]


def last_page_by_user_session():
    # df = get_data_from_db(query_user_journey)
    all_pages = set()
    last_page_counts = {}
    df = pd.read_csv('user_anal_db.csv').fillna(0)
    filtered_df = df.query(
        "product_time > 9 and redirected > 0 and (funnel_id == 1 or funnel_id == 8)"
    )
    print(filtered_df)

    for _, row in filtered_df.iterrows():
        try:
            funnel_id = row['funnel_id']
            redirected = int(row['redirected'])
            session = str(row['session_number'])
            user_journey = str(row['journey_pages']).split(',')
            general_user_journey = general_pages(row['journey_pages'])
            all_pages.update(user_journey, general_user_journey)
            last_step = general_user_journey[-1]
            # print(row['user_id'], last_step, redirected)
            if row['journey_len'] != 1 and user_journey[0] != user_journey[-1]:
                second_step = general_user_journey[1]
                last_page_counts.setdefault(funnel_id, {}).setdefault(session, {}).setdefault(second_step, 0)
                last_page_counts[funnel_id][session][second_step] += 1
            # elif row['journey_len'] == 1:
            #     last_page_counts.setdefault(funnel_id, {}).setdefault(session, {}).setdefault(last_step, 0)
            #     last_page_counts[funnel_id][session][last_step] += 1
        except Exception as ex:
            print(ex, row['user_id'])
            # print(str(ex))
            pass
    print(json.dumps(last_page_counts, indent=3))
    last_page_counts_list = []
    for funnel_id, last_page_data in last_page_counts.items():
        for session, page_data in last_page_data.items():
            for page, count in page_data.items():
                last_page_counts_list.append((funnel_id, session, page, count))

    df_result = pd.DataFrame(last_page_counts_list, columns=['funnel_id', 'session', 'page_name', 'count'])
    df_result = df_result.sort_values(by=['funnel_id', 'session', 'count'], ascending=[True, True, False])

    df_result.to_csv('second_payed_page.csv', index=False)


def after_chat_message_limit():
    last_page_counts = {}
    all_pages = set()
    df = pd.read_csv('query_user_journey_after_chat_message_limit_reached.csv')
    print(df)
    for _, row in df.iterrows():
        funnel_id = row['funnel_id']
        session = str(row['session_number'])
        user_journey = str(row['journey_pages']).split(',')
        general_user_journey = general_pages(row['journey_pages'])
        all_pages.update(user_journey, general_user_journey)
        last_step = general_user_journey[-1]
        if len(user_journey) != 1 and user_journey[0] != user_journey[-1]:
            second_step = general_user_journey[-1]
            last_page_counts.setdefault(funnel_id, {}).setdefault(session, {}).setdefault(second_step, 0)
            last_page_counts[funnel_id][session][second_step] += 1
        elif len(user_journey) == 1:
            last_page_counts.setdefault(funnel_id, {}).setdefault(session, {}).setdefault(last_step, 0)
            last_page_counts[funnel_id][session][last_step] += 1
    print(last_page_counts)
    last_page_counts_list = []
    print('here')
    for funnel_id, last_page_data in last_page_counts.items():
        for session, page_data in last_page_data.items():
            for page, count in page_data.items():
                last_page_counts_list.append((funnel_id, session, page, count))

    df_result = pd.DataFrame(last_page_counts_list, columns=['funnel_id', 'session', 'page_name', 'count'])
    df_result = df_result.sort_values(by=['funnel_id', 'session', 'count'], ascending=[True, True, False])

    df_result.to_csv('after_limit_chat.csv', index=False)


all_pages = set()
last_page_counts = {}
df = pd.read_csv('users_analytics.csv').fillna(0)
filtered_df = df.query(
    "product_time > 9 and redirected == 0 and banner_clicks == 0 "
    "and attr_clicks == 0 and  funnel_id == 8 ")

# filtered_df = df.query(
#     "product_time > 9 and banner_clicks == 0 and attr_clicks == 0 and redirected == 0 "
#     "and session_number <= 5 and (funnel_id ==1 or funnel_id ==8)")


filtered_df = filtered_df[~filtered_df['journey_pages'].str.split(',').str[-1].fillna('').str.contains('chats')]

# filtered_df = df.query(
#     "product_time > 9 and redirected > 0 and funnel_id == 1 and session_number == 1 and user_id not in @df.query('session_number > 1')['user_id'].unique()")

# filtered_df = df.query("redirected > 0 and (funnel_id == 1 or funnel_id == 8)")
print(filtered_df)
print()
# for _, row in filtered_df.iterrows():
#     funnel_id = row['funnel_id']
#     redirected = int(row['redirected'])
#     session = str(row['session_number'])
#     user_journey = str(row['journey_pages']).split(',')
#
#     general_user_journey = general_pages(row['journey_pages'])
#     all_pages.update(user_journey, general_user_journey)
#     last_step = general_user_journey[-1]
#     # print(row['user_id'], last_step, redirected)
#     if 'chats[' in user_journey[-1]:
#         step = general_user_journey[-1]
#         last_page_counts.setdefault(funnel_id, {}).setdefault(session, {}).setdefault(step, 0)
#         last_page_counts[funnel_id][session][step] += 1
#
# print(last_page_counts)

conversion_counts = filtered_df.groupby(['funnel_id', 'user_source']).agg({
    'redirected': ('sum', 'mean'),
    'user_id': 'nunique'
})
print(conversion_counts.to_csv('delete.csv'))
