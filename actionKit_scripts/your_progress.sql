/* Calculates the number of users that have taken an action since the beginning of Color of Change*/
/* The code leverages SQL but also HTML because the widget is embedded on ActionKits Your Progress Page.
The HTML code inserts the beginning and ending dates for the report*/ 

select count(*) as "user_count"

from core_user

/* Returns a user-row as long as they are not labeled spam and has taken more than 1 action*/ 
where exists (select core_action.user_id 
              from core_action 
              where core_action.status != 'spam'  
              group by 1 having count(*) >0)

/* Joins core_user with core_subsriptions and checks the users is not in the following lists:
  - 16 = Fake User Listing for Server Testing  
  - 17 = Spambot Trap to collect spambot userids
  - 18 = Return Path Seed List
  - 35 = Inactive Ready for Re-Engagement List */
and exists (select core_subscription.list_id 
            from core_subscription 
            where core_subscription.user_id = core_user.id 
            and core_subscription.list_id not in (16,17,18,35 ))

/* Counts users that have not been imported by a staff member, status is complete, and has taken more 
   than one action*/                                                  
and core_user.id in (select core_action.user_id 
                     from core_action join core_page on core_action.page_id = core_page.id 
                     where core_action.status = 'complete' 
                     and core_page.type not in ('Import') 
                     group by 1 having count(*) >0)

/* Returns a user that that meets the following data cleaning criteria */                                                  
and exists (select core_user.id as "user_id",
                   core_user.email as "user_email",
                   core_user.first_name as "first_name",
                   core_user.last_name as "last_name"
    
             from core_user
             /* Excludes email addresses that have the incorrect suffix */
             where core_user.email NOT REGEXP ".con$"| "ymail.com$" | ".con$" |"yahoo.con$"| '[&=+<>]' |'^[a-zA-z]'

             /* Checks if the first name and last name are the same and excludes those users*/
             and ((NULLIF (core_user.first_name, core_user.last_name) IS NOT NULL) 
                                                  
             /* Checks that the first or last name value is not empty*/
             or ('first_name' IS NOT NULL) or ('last_name' IS NOT NULL)) 
             
             /* Checks the length of each first name and last name and excludes any name with 10 or more characters*/
             and (( LENGTH (core_user.first_name) >= 10 and LENGTH (core_user.last_name) = 0) 
             or (LENGTH (core_user.last_name) >= 10 and LENGTH (core_user.first_name) = 0))

             group by core_user.id)


/*HTML codes that is used to insert the parameters for the different dates to run this report  */
{% if partial_run %}
and core_user.created_at between {last_run} and {now}
{% endif %}
;

