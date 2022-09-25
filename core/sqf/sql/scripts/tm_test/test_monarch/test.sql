sh pstat; 
set schema trafodion.seabase;
prepare insert_stmt from insert into inventory values (?, 2, 3);
prepare li1_stmt from insert into LI1 values (?, 100);
prepare li2_stmt from insert into LI2 values (?, 200);
prepare li3_stmt from insert into LI3 values (?, 300);
prepare li1_sel  from select * from LI1 where a = ?;
prepare li2_sel  from select * from LI2 where a = ?;
prepare li3_sel  from select * from LI3 where a = ?;
begin work;
execute insert_stmt using 1;
execute li1_stmt using 1;
execute li2_stmt using 1;
execute li3_stmt using 1;
execute li1_sel using 1;
execute li2_sel using 1;
execute li3_sel using 1;
commit work;

