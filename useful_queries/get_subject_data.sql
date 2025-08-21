

select 

*,
ss.subject_fullname as new_subject_fullname,
case when  received is null then ss.water_per_day  
	 when  ss.water_per_day > received then ROUND(ss.water_per_day - received,1)
	 else 0 end as suggested_water


from u19_action.subject_status ss 


left join u19_subject.subject_rfid sr 
on sr.subject_fullname = ss.subject_fullname


inner join
(
select 

subject_fullname,
max(effective_date) as last_status

from u19_action.subject_status ss 

group by subject_fullname
) max_status
on max_status.subject_fullname = ss.subject_fullname
and max_status.last_status = ss.effective_date


inner join u19_subject.subject s
on s.subject_fullname = ss.subject_fullname

inner join u19_subject.caging_status cs 
on cs.subject_fullname = ss.subject_fullname



inner join
(
select 
user_id as new_user_id,
slack
from u19_lab.user
) us
on us.new_user_id = s.user_id

left join
(
select
*
from u19_subject.health_status
where status_date = CURDATE()
) hs
on hs.subject_fullname = ss.subject_fullname

left join
(
select 

subject_fullname
,date(wai.administration_time) as administration_date_individual
,sum(case when administation_type= 'earned' then water_amount else 0 end) as earned
,sum(case when administation_type<> 'earned' then water_amount else 0 end) as supplement
,sum(water_amount) as received


from u19_action.water_administration_individual wai

where date(wai.administration_time) = CURDATE()

group by subject_fullname, date(wai.administration_time)

) water_admin_ind
on water_admin_ind.subject_fullname = ss.subject_fullname


left join
(
select 

subject_fullname
,case when already_received = 0 then prescribed_extra_supplement_amount else 0 end as prescribed_extra_supplement_amount
,already_received

from u19_action.extra_supplement_water

where extra_supplement_water_date = CURDATE()


) esw
on esw.subject_fullname = ss.subject_fullname



left join
(
select 
w.subject_fullname,
w.weight


from u19_action.weighing w

inner join
(
select 

w.subject_fullname,
max(w.weighing_time) as last_weigh

from u19_action.weighing w

where date(w.weighing_time) = CURDATE()

group by subject_fullname
) max_weigh
on max_weigh.subject_fullname = w.subject_fullname
and max_weigh.last_weigh = w.weighing_time


) weighing_full
on weighing_full.subject_fullname = ss.subject_fullname


left join
(
select 
w.subject_fullname,
w.weighing_time as last_weighing_time,
w.weight as last_weight,
wp.full_name as last_weigh_person_name

from u19_action.weighing w

inner join
(
select 

w.subject_fullname,
max(w.weighing_time) as last_weigh

from u19_action.weighing w

group by subject_fullname
) max_weigh2
on max_weigh2.subject_fullname = w.subject_fullname
and max_weigh2.last_weigh = w.weighing_time


inner join u19_lab.user wp
on wp.user_id = w.weigh_person

) weighing_full2
on weighing_full2.subject_fullname = ss.subject_fullname


left join(
select 

sam.subject_fullname,
GROUP_CONCAT(sam.act_item ORDER BY sam.act_item SEPARATOR '\n') as current_actions


from u19_subject.subject_action_manual sam 

where date(notification_date) < curdate() and date(valid_until_date) >= curdate()

group by subject_fullname

) actions_subject
on actions_subject.subject_fullname = ss.subject_fullname

left join(
select 

ar.subject_fullname,
GROUP_CONCAT(ar.action ORDER BY ar.action SEPARATOR '\n') as done_actions


from u19_action.action_registry ar  

where date(action_datetime) = curdate()

group by subject_fullname

) actions_done_subject
on actions_done_subject.subject_fullname = ss.subject_fullname


left join 
(
select
subject_fullname,
min(session_location) as session_location

from u19_acquisition.session_started


where session_date = curdate()


group by subject_fullname

) session_subject
on session_subject.subject_fullname = ss.subject_fullname


left join 
(
select 
subject_fullname,
timeslot,
location as scheduled_rig

from u19_scheduler.schedule s 

where date = curdate()
) schedule_subject
on schedule_subject.subject_fullname = ss.subject_fullname



where ss.subject_status not in ("Dead","Missing", "AdLibWater")

order by s.user_id, UPPER(cs.cage), ss.subject_fullname

