// 

select count(*) as "user_count"

from core_user
where exists (select core_action.user_id 
              from core_action 
              where core_action.status != 'spam'  
              group by 1 having count(*) >0)

and exists (select core_subscription.list_id 
            from core_subscription 
            where core_subscription.user_id = core_user.id 
            and core_subscription.list_id not in (16,17,18,35 ))

and core_user.id in (select core_action.user_id 
                     from core_action join core_page on core_action.page_id = core_page.id 
                     where core_action.status = 'complete' 
                     and core_page.type not in ('Import') 
                     group by 1 having count(*) >0)

and exists (select core_user.id as "user_id",
                   core_user.email as "user_email",
                   core_user.first_name as "first_name",
                   core_user.last_name as "last_name"
    
             from core_user
             where core_user.email NOT REGEXP ".con$"| "ymail.com$" | ".con$" |"yahoo.con$"| '[&=+<>]' |'^[a-zA-z]'

             and ((NULLIF (core_user.first_name, core_user.last_name) IS NOT NULL) or ('first_name' IS NOT NULL) or ('last_name' IS NOT NULL)) 

             and (( LENGTH (core_user.first_name) >= 10 and LENGTH (core_user.last_name) = 0) 
             or (LENGTH (core_user.last_name) >= 10 and LENGTH (core_user.first_name) = 0))

             group by core_user.id)



{% if partial_run %}
and core_user.created_at between {last_run} and {now}

{% endif %}
;

