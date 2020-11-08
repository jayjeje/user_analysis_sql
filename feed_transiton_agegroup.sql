
WITH data as (
    select 
        distinct 
        user_dim.user_id,
        params.value.int_value as session,
        name as event_name,
        datetime(timestamp_micros(timestamp_micros)) as event_time
        
    from 
        `app_events_*`,
        UNNEST(event_dim) as event,
        UNNEST(event.params) as params
    where
        _table_suffix
            between format_datetime('%Y%m%d', datetime({{start_date}}))
            and format_datetime('%Y%m%d', datetime({{end_date}}))
        AND event.name IN (
        'click_style_feed',
        'visit_hot_feed',
        'visit_my_profile',
        'visit_notification_list',
        'visit_new_feed',
        'visit_following_feed',
        'visit_store_home',
        'visit_dailylook_feed',
        'visit_secondhand_feed',
        'visit_beauty_feed'
        )
        AND params.key in  ('ga_session_id')
        AND user_dim.user_id IS NOT NULL
)
,
-- user age 붙히기 

firebase_user_birthyear_data as (
  select 
    distinct user_id, 
    string_value as birthyear
  from `firebase.user_profiles_*`
  where key = 'birthyear'
),

firebase_user_profiles_data as (
  select 
    user_id,
    firebase_user_birthyear_data.birthyear as birthyear, 
    EXTRACT(year FROM current_date()) - cast(birthyear as int64) + 1 as kr_age
  from firebase_user_birthyear_data
),

firebase_user_age_group_data as (
    select user_id, age_group 
    from (
        select
            *,
            CASE
                WHEN kr_age between 10 and 19 THEN '10~19'
                WHEN kr_age between 20 and 21 THEN '20~21'
                WHEN kr_age between 22 and 23 THEN '22~23'
                WHEN kr_age between 24 and 26 THEN '24~26'
                WHEN kr_age between 27 and 29 THEN '27~29'
                WHEN kr_age >=30 THEN '30+'
            ELSE '기타'
          END
          AS age_group
        from firebase_user_profiles_data
        WHERE kr_age>=10
    )
)
,

check1 as (
select user_id, event_name, event_time, session
from ( 
    select 
        user_id, session, event_name, event_time,
        case when event_name = LEAD(event_name,1,'--') over(partition by user_id, session order by event_time) then 0 else 1 end as check
        
    from data  
    )
where check = 1
),

check_age as ( 
    select  
        b.age_group,
        b.user_id,
        a.event_name,
        a.event_time,
        a.session
    from check1 a join firebase_user_age_group_data b 
    on a.user_id=b.user_id

),

move as (
    select 
        age_group, user_id, session, event_time, event_name, 
        LEAD(event_name,1) over(partition by user_id, session order by event_time) as next_action1,
        LEAD(event_name,2) over(partition by user_id, session order by event_time) as next_action2,
        LEAD(event_name,3) over(partition by user_id, session order by event_time) as next_action3,
        LEAD(event_name,4) over(partition by user_id, session order by event_time) as next_action4,
        LEAD(event_name,5) over(partition by user_id, session order by event_time) as next_action5,
        LEAD(event_name,6) over(partition by user_id, session order by event_time) as next_action6,
        row_number() over(partition by user_id, session order by event_time) as rn
    from check_age
   
),

final as (
    select *
    from move
    where rn =1
    order by 1, 2
    )
    

 
select 
    *
from final


