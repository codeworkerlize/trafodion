set schema seabase;
select count(*) from inventory;
select count(*) from li1;
select count(*) from li2;
select count(*) from li3;

select min(item_id) from inventory;
select min(a) from li1;
select min(a) from li2;
select min(a) from li3;

select max(item_id) from inventory;
select max(a) from li1;
select max(a) from li2;
select max(a) from li3;
