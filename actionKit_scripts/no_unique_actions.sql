-- Returns the number of users that have not taken an action within ( months 
-- Also eliminates users that were not imported into ActionKit and the subscription status is subscribed 
-- script ran in SQL, NOT MySQL

select distinct core_subscription.user_id from coc_ak.core_subscription 

join coc_ak.core_user on (core_subscription.user_id = core_user.id)
join coc_ak.core_action on (coc_ak.core_user.id = core_action.user_id) 

where 
  (select count(*) from coc_ak.core_action as ca where ca.user_id = core_user.id 
  and ca.source <> 'import' and ca.status = 'complete' and updated_at >= current_date - interval '9 month') < 1
  and core_subscription.user_id not in (select user_id from coc_ak.core_open where core_open.created_at >= current_date - interval '9 month' )
  and core_subscription.user_id not in (select user_id from coc_ak.core_order where core_order.created_at >= current_date - interval '9 month')
