with pre as 
(
    select 
        date_trunc(date(timestamp_micros(timestamp_micros),'Asia/Seoul'), week(monday)) as week,
        user_id,
        count(timestamp_micros(timestamp_micros)) as visited_count
    FROM `user_events_*` 
    where _TABLE_SUFFIX 
            BETWEEN FORMAT_DATETIME("%Y%m%d", DATETIME({{start_date}}))
                AND FORMAT_DATETIME("%Y%m%d", DATETIME({{end_date}}))
        AND event_name in ('see_style_feed','open_marketing_push','open_deep_link', 'open_with_app_link')
    group by 1, 2

),

first_w as 

(
    select 
        user_id,
        min(week) as first_week

    from pre
    group by 1
),

pre_f as 
(
    select 
        pre.week,
        pre.user_id,
        pre.visited_count,
        first_w.first_week
    from pre left join first_w 
        on pre.user_id=first_w.user_id
)
,

--user age 

birthyear_data as (
    select 
        distinct user_id,
        string_value as birthyear
    from `firebase.user_profiles_*`
    where key= 'birthyear'

)

,profile_data as (
    select 
        user_id,
        extract(year from current_date())-cast(birthyear as int64) + 1 as kr_age
    from birthyear_data

)
, age_group_data as (
    select
        user_id,
         case 
            when kr_age BETWEEN 10 and 19 then '10~19'
            when kr_age BETWEEN 20 and 21 then '20~21'
            when kr_age BETWEEN 22 and 23 then '22~23'
            when kr_age BETWEEN 24 and 26 then '24~26'
            when kr_age BETWEEN 27 and 40 then '27~40'
            when kr_age BETWEEN 41 and 60 then '41~60'
            Else '기타'
         End as age_group 
    from profile_data
    where kr_age >= 10

),

pre_first as (
    select 
        age_group,
        week,
        pre_f.user_id,
        visited_count,
        first_week
        
    from pre_f join age_group_data on pre_f.user_id=age_group_data.user_id


)      


select 
        tm.age_group,
        coalesce(tm.week, date_add(lm.week, interval 1 week)) as week,
        count(distinct tm.user_id) as wau,
        count(distinct case when lm.user_id is not NULL then tm.user_id else NULL end) as retained,
        count(distinct case when tm.first_week = tm.week then tm.user_id  else NULL end) as new_user,
        count(distinct case when tm.first_week != tm.week
                and lm.user_id is NULL then tm.user_id else NULL end) as resurrected,
        count(distinct case when tm.user_id is NULL then lm.user_id
                else NULL end) as dormant
from pre_first tm FULL OUTER JOIN pre_first lm
    on tm.user_id=lm.user_id 
        and tm.week=date_add(lm.week, interval 1 week)
group by 1, 2
order by 1, 2