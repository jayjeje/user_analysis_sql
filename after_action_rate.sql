with events_list as 
(
    
    SELECT *
    FROM UNNEST((SPLIT({{event_names}}, ','))) AS element
 ),

data as (
    SELECT
        user_dim.user_id as id,
        DATETIME(TIMESTAMP_MICROS(timestamp_micros), 'Asia/Seoul') as event_time,
        lead(DATETIME(TIMESTAMP_MICROS(timestamp_micros), 'Asia/Seoul')) over (partition by user_dim.user_id order by DATETIME(TIMESTAMP_MICROS(timestamp_micros), 'Asia/Seoul')) as event_time2,
        name,
        lead(name,1) over (partition by user_dim.user_id order by DATETIME(TIMESTAMP_MICROS(timestamp_micros), 'Asia/Seoul')) as event_name2
    FROM
    `app_events_*`,
    UNNEST(event_dim) as event,
    UNNEST(event.params) as event_params
    WHERE _TABLE_SUFFIX 
        BETWEEN FORMAT_DATETIME("%Y%m%d", DATETIME_SUB(DATETIME({{ end_date }}, 'Asia/Seoul'),INTERVAL {{day}} DAY))
            AND FORMAT_DATETIME("%Y%m%d",DATETIME({{ end_date }}, 'Asia/Seoul'))
        AND name in (select * from events_list)
        and user_dim.platform='ios'
order by 1, 2 
),
action1 as (
    select 
        id as id1,
        event_time
    from data
    where  event_name2=(SPLIT({{event_names}}, ','))[offset(1)]
),
action2 as (
    select 
        id as id2,
        event_time
    from data
    where event_name2=(SPLIT({{event_names}}, ','))[offset(2)]
),
action1_join as (
Select 
    data.event_time,
    data.id,
    action1.id1
from data left join action1 
    on data.id = action1.id1
    and data.event_time=action1.event_time
)
select 
    datetime_trunc(action1_join.event_time, week(monday)) as event_week,
    count(distinct id1)/count(distinct id) as action1_after_link,
    count(distinct id2)/count(distinct id) as action2_after_link
from action1_join  left join action2 
    on action1_join.id=action2.id2
    and action1_join.event_time= action2.event_time
group by 1
order by 1