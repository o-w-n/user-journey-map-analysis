import pandas as pd
import matplotlib.pyplot as plt
from config import get_data_from_db
from loguru import logger

from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth

q1 = """
      SELECT base.user_id,
       base.session_number,
       max(base.offer_id)                                                                         as offer_id,
       max(o.offer_category_id)                                                                   as offer_category_id,
       max(base.os)                                                                               as os,
       max(base.device_vendor)                                                                    as device_vendor,
       MAX(base.conversion_funnel_id)                                                             AS funnel_id,
       CASE
           WHEN REPLACE(ARRAY_TO_STRING(ARRAY_AGG(base.page_name ORDER BY base.created), ','), 'null', '') = '' THEN 0
               WHEN REPLACE(ARRAY_TO_STRING(ARRAY_AGG(base.page_name ORDER BY base.created), ','), 'null', '') <> '' THEN
               regexp_count(
                       REPLACE(ARRAY_TO_STRING(ARRAY_AGG(base.page_name ORDER BY base.created), ','), 'null', ''), ','
                   )+1
           ELSE 1 END as journey_len,
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
                 WHEN ue.event = 'route' or ue.event = 'user_visit' and LENGTH(ue.page) > 19 AND LENGTH(ue.page) < 50 THEN
                     CASE
                         WHEN regexp_count(ue.page, '/') = 3 THEN
                             CASE
                                 WHEN ue.page NOT LIKE '%id=%' THEN COALESCE(
                                         trim(split_part(split_part(ue.page, '/', 4), '?', 1)), '')
                                 ELSE CONCAT(
                                         COALESCE(trim(split_part(split_part(ue.page, '/', 4), '?', 1)), ''),
                                         '[',
                                         COALESCE(trim(split_part(split_part(ue.page, '/', 4), 'id=', 2)), ''),
                                         ']'
                                     )
                                 END
                         WHEN regexp_count(ue.page, '/') = 4 THEN
                             CASE
                                 WHEN split_part(ue.page, '/', 4) LIKE '%profile%' or
                                      split_part(ue.page, '/', 4) LIKE '%matches%' THEN COALESCE(
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
      WHERE ue.created::date >= current_date - interval '7 day'
        and uv.session_start::date >= current_date - interval '7 day'
      ORDER BY ue.user_id, ue.created) base
         left join offer o ON base.offer_id::integer = o.id
GROUP BY base.user_id, base.session_number;
"""

q2 = """

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
       ARRAY_AGG(ROW (base.event,base.page_name, base.created::date))                             AS page_info_array,

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
                                 ELSE trim(split_part(split_part(ue.page, '/', 4), '?', 1))
                                 /*CONCAT(
                                         COALESCE(trim(split_part(split_part(ue.page, '/', 4), '?', 1)), ''),
                                         '[',
                                         COALESCE(trim(split_part(split_part(ue.page, '/', 4), 'id=', 2)), ''),
                                         ']'
                                     )*/
                                 END
                         WHEN regexp_count(ue.page, '/') = 4 THEN
                             CASE
                                 WHEN split_part(ue.page, '/', 4) LIKE '%profile%' or
                                      split_part(ue.page, '/', 4) LIKE '%matches%' THEN COALESCE(
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
      WHERE ue.created::date >= current_date - interval '7 day'
        and uv.session_start::date >= current_date - interval '7 day'
      ORDER BY ue.user_id, ue.created) base
         left join offer o ON base.offer_id::integer = o.id
         left join (select user_id, sum(time_difference) as product_time
                    from (select user_id,
                                 EXTRACT(EPOCH FROM (ue.created - LAG(ue.created, 1) OVER
                                     (PARTITION BY user_id ORDER BY ue.created)))
                                     AS time_difference
                          from "user" u
                                   left join user_event ue on u.id = ue.user_id
                          WHERE ue.created::date >= current_date - interval '7 day'
                            and ue.created <= u.created + interval '24 hours'
                            and u.device = 'mobile'
                            and ue.event not in ('user_afk', 'user_socket_disconnect')) time_diff
                    where time_difference <= 900
                    group by 1) pt on pt.user_id = base.user_id
GROUP BY base.user_id, base.session_number;
;
"""
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)
pd.set_option('display.max_rows', 2000)


def stat_users_visits(df):
    df['journey_pages'] = df['journey_pages'].str.split(',')
    page_counts = {}
    for pages in df['journey_pages']:
        for page in pages:
            if '[' in page:
                page = page.split('[')[0].strip()
            else:
                page = page.strip()
            if page in page_counts:
                page_counts[page] += 1
            else:
                page_counts[page] = 1
    pages_df = pd.DataFrame({'Page': list(page_counts.keys()), 'Visits': list(page_counts.values())})
    pages_df = pages_df.sort_values(by='Visits', ascending=False)
    logger.info(f'Top 5 most visited pages: {pages_df.head(5)}')

    plt.figure(figsize=(10, 6))
    plt.bar(pages_df['Page'], pages_df['Visits'])
    plt.xticks(rotation=90)
    plt.xlabel('Page')
    plt.ylabel('Visits')
    plt.title('Page Visits')
    plt.show()


def patterns_apriori(df):
    te = TransactionEncoder()
    data = te.fit_transform(df)
    print(data)
    df_encoded = pd.DataFrame(data, columns=te.columns_)
    frequent_patterns = fpgrowth(df_encoded, min_support=0.01, use_colnames=True)

    frequent_patterns = frequent_patterns.sort_values(by='support', ascending=False)
    print(frequent_patterns)
    return frequent_patterns


# df = get_data_from_db(q2)

df = pd.read_csv('user_anal_db.csv')

# df_reset = df.reset_index()
# top_companies = df.groupby(['funnel_id', 'session_number', 'offer_category_id', 'offer_id'])[
#     'redirected'].sum().nlargest(int(len(df) * 0.2)).index
#
# filtered_df = df.reset_index().set_index(['funnel_id', 'session_number', 'offer_category_id', 'offer_id'])
#
# top_20percent_users = filtered_df.loc[
#                       (filtered_df.index.isin(top_companies)) & (filtered_df['redirected'] > 0), :]
#
# paid_users = filtered_df.loc[
#              (filtered_df.index.isin(top_companies)) & (filtered_df['redirected'] > 0), :]
# other_80percent_users = filtered_df.loc[
#                         (~filtered_df.index.isin(top_companies)) & (filtered_df['redirected'] <= 0), :]
#
# empty_users = filtered_df.loc[~filtered_df.index.isin(top_companies) & (filtered_df['journey_pages'] == '')]
#
# test = filtered_df.loc[(filtered_df.index.isin(top_companies)), :]
#
# print(top_20percent_users.to_csv('top_users'))
# print(paid_users)
# print(other_80percent_users)
# print(empty_users)

# stat_users_visits(top_20percent_users)
#
#
# df['journey_pages'] = df['journey_pages'].str.split(',')
#


pattern_df = df.query(
    "product_time > 9 and redirected > 0 "
    "and session_number <= 5 and (funnel_id == 8)"
)
pattern_df['journey_pages'] = pattern_df['journey_pages'].apply(
    lambda x: str(x).split('[')[0].strip() if pd.notnull(x) else '')
pattern_df['journey_pages'] = pattern_df['journey_pages'].str.split(',')

patterns_result = patterns_apriori(pattern_df['journey_pages'])
print(pattern_df)
