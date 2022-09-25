CREATE PROCEDURE trafodion.sch.set_message(IN name varchar(20), OUT s_result varchar(40)) as//
BEGIN
 SET s_result = 'Hello, ' || name || '!';
 dbms_output.put_line( s_result);
END//
;
