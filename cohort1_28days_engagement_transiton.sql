with new_users as (
    SELECT 
        cast(id as STRING) as id, 
        datetime(created_at, 'Asia/Seoul') as reference_date
    from `new_users_*`
    where  _TABLE_SUFFIX 
            BETWEEN FORMAT_DATETIME("%Y%m%d", DATETIME_SUB(DATETIME({{end_date}}), INTERVAL 28 DAY))
                AND FORMAT_DATETIME("%Y%m%d", DATETIME_ADD(DATETIME({{end_date}}), INTERVAL 0 HOUR))
        and datetime(created_at, 'Asia/Seoul')
            BETWEEN DATETIME_SUB(DATETIME({{end_date}}), INTERVAL 28 DAY)
                AND DATETIME_ADD(DATETIME({{end_date}}), INTERVAL 0 HOUR)
),

data as (
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
    where  _TABLE_SUFFIX 
            BETWEEN FORMAT_DATETIME("%Y%m%d", DATETIME_SUB(DATETIME({{end_date}}), INTERVAL 28 DAY))
                AND FORMAT_DATETIME("%Y%m%d", DATETIME_ADD(DATETIME({{end_date}}), INTERVAL 0 HOUR))
    and datetime(timestamp_micros(timestamp_micros),'Asia/Seoul')
            BETWEEN DATETIME_SUB(DATETIME({{end_date}}), INTERVAL 28 DAY)
                AND DATETIME_ADD(DATETIME({{end_date}}), INTERVAL 0 HOUR)
        AND name IN ( 'click_buy_confirm_button', 'click_add_wishlist', 'click_goods_detail_add_cart', 'click_like','click_follow', 'click_collect', 
                        'click_upload_button', 'click_upload_hashtage_recommend', 'create_style_success',
                        'create_review_success', 'create_comment', 'click_recommend_user_follow', 'click_onboarding_recom_user_follow', 
                        'click_recommend_user_follow_button','click_follow_back_button',  'create_info_comment',
                        'send_shoppable_live_message', 'visit_order_complete', 'download_picture', 'take_screenshot')
        AND params.key in  ('ga_session_id')
        AND user_dim.user_id not in (select id from new_users)
        
)
,

check1 as (
select user_id, event_name, event_time, session
from ( 
    select 
        user_id, session, event_name, event_time,
        case when event_name = LEAD(event_name,1,'--') over(partition by user_id, session order by event_time) then 0 else 1 end as check
        -- LEAD(event_time,1) over(partition by user_id, session order by event_time) as next_time1,
    from data  
    )
where check = 1
),

move as (
    select 
        user_id, session, event_name, event_time,
        LEAD(event_name,1,'--') over(partition by user_id, session order by event_time) as next_action1,
        LEAD(event_name,2,'--') over(partition by user_id, session order by event_time) as next_action2,
        LEAD(event_name,3,'--') over(partition by user_id, session order by event_time) as next_action3,
        LEAD(event_name,4,'--') over(partition by user_id, session order by event_time) as next_action4,
        LEAD(event_name,5,'--') over(partition by user_id, session order by event_time) as next_action5,
        LEAD(event_name,6,'--') over(partition by user_id, session order by event_time) as next_action6,
        row_number() over(partition by user_id, session order by event_time) as rn
    from check1 
   
),

final as (
    select *
    from move
    where rn =1
    order by 1, 2
    )
    
select *
from final
-- select event_name,
--         count(event_time) as cnt
-- from final
-- group by 1 
-- order by 2 desc







-- select 
--     distinct next_action1,
--     count(session) as cnt
-- from final
-- where event_name='visit_hot_feed'
-- -- and next_action1='visit_notification_list'
-- -- and next_action2='visit_notification_list'
-- -- and next_action3='visit_notification_list'
-- -- and next_action4='visit_notification_list'
-- -- and next_action5='visit_notification_list'
-- group by next_action1
-- order by cnt desc


