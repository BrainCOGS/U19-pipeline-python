

select 

tb.subject_fullname,
tb.session_date,
tb.session_number,
ts.subject_fullname,
tb.session_location,
tb.num_trials,
tb.is_bad_session

from u19_acquisition.`session` tb 

left join u19_behavior.`_towers_block` ts 
on tb.subject_fullname = ts.subject_fullname 
and tb.session_date = ts.session_date 
and tb.session_number = ts.session_number 


where ts.subject_fullname is null and tb.session_date < CURDATE() and tb.task = 'Towers' and tb.subject_fullname not like 'testuser%' and tb.is_bad_session = 0

order by session_date desc

