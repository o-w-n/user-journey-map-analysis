import pandas as pd
from config import get_data_from_db

query = """
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
                 WHEN ue.event = 'route' and LENGTH(ue.page) > 19 AND LENGTH(ue.page) < 50 THEN
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
"""

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
pd.set_option('display.max_rows', 2000)

data_db = pd.read_csv('user_anal_db.csv').fillna(0).query(
    "product_time > 9 and  redirected > 0 and (funnel_id == 1 or funnel_id == 8)"
)


def stats_by_funnels(df: pd.DataFrame, grouped_columns: list, order_by: list,
                     asc: bool = False) -> pd.DataFrame:
    total_users = df.groupby(grouped_columns)['user_id'].nunique()
    conversion_counts = df.groupby(grouped_columns).agg({
        'banner_clicks': 'sum',
        'attr_clicks': 'sum',
        'redirected': 'sum',
        # 'offer_category_id': ('offer_category_id', 'max'),
        'journey_len': 'mean',
        'product_time': 'mean'
    })
    conversion_counts['total_users'] = total_users
    conversion_counts['total_clicks'] = conversion_counts['banner_clicks'] + conversion_counts['attr_clicks']

    conversion_percentages = pd.DataFrame()
    conversion_percentages[
        'banner_clicks'] = round((conversion_counts['banner_clicks'] / conversion_counts['total_users']) * 100)
    conversion_percentages[
        'attr_clicks'] = round((conversion_counts['attr_clicks'] / conversion_counts['total_users']) * 100)
    conversion_percentages[
        'total_clicks'] = round((conversion_counts['total_clicks'] / conversion_counts['total_users']) * 100)

    conversion_percentages = conversion_percentages.rename(columns={
        'total_clicks': 'Total Click %',
        'banner_clicks': 'Banner Click %',
        'attr_clicks': 'Attribute Click %',
        'redirected': 'Redirected %'

    })
    conversion_counts = conversion_counts.rename(columns={
        'total_users': 'Total User',
        'journey_len': 'Journey len',
        'banner_clicks': 'Banner Click',
        'attr_clicks': 'Attribute Click',
        'total_clicks': 'Total Click',
        'redirected': 'Redirected'

    })

    result_df = pd.concat([conversion_counts, conversion_percentages], axis=1)
    return result_df.sort_values(by=order_by, ascending=asc)


result_funnel_id = stats_by_funnels(df=data_db, grouped_columns=['funnel_id'],
                                    order_by=['funnel_id'], asc=True)

result_offer_offer_category_id = stats_by_funnels(df=data_db, grouped_columns=['offer_category_id'],
                                                  order_by=['offer_category_id'], asc=True)
result_session_number = stats_by_funnels(df=data_db, grouped_columns=['session_number'],
                                         order_by=['session_number'], asc=True)

result_funnel_sessions = stats_by_funnels(df=data_db, grouped_columns=['funnel_id', 'session_number'],
                                          order_by=['funnel_id', 'session_number'], asc=True)
result_screen_size = stats_by_funnels(df=data_db, grouped_columns=['screen_size'],
                                      order_by=['screen_size'], asc=True)

result_funnel_id_session_offer_category_id_offer_id = stats_by_funnels(df=data_db,
                                                                       grouped_columns=['funnel_id', 'session_number',
                                                                                        'offer_category_id'],
                                                                       order_by=['funnel_id', 'session_number',
                                                                                 'Total User'], asc=True)
