

ALTER TABLE u19_behavior.`_towers_block`  DROP FOREIGN KEY `_towers_block_ibfk_3`;
 
ALTER TABLE u19_behavior.`_towers_block` ADD CONSTRAINT _towers_block_ibfk_3 FOREIGN KEY (`subject_fullname`,`session_date`,`session_number`) 
REFERENCES `u19_behavior`.`_towers_session` (`subject_fullname`,`session_date`,`session_number`) ON UPDATE CASCADE ON DELETE RESTRICT


ALTER TABLE u19_behavior.`_towers_block__trial`  DROP FOREIGN KEY `_towers_block__trial_ibfk_0`;

ALTER TABLE u19_behavior.`_towers_block__trial` ADD CONSTRAINT _towers_block__trial_ibfk_0 FOREIGN KEY (`subject_fullname`,`session_date`,`session_number`,`block`) 
REFERENCES `u19_behavior`.`_towers_block` (`subject_fullname`,`session_date`,`session_number`,`block`) ON UPDATE CASCADE ON DELETE RESTRICT
