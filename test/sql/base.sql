set pg_orca.enable_orca to off;

create extension pg_tpch;

 create table product
  (
          pn int not null,
          pname text not null,
          pcolor text,
 
          primary key (pn)
 
  ) ;
 
  create table sale
  (
          cn int not null,
          vn int not null,
          pn int not null,
         dt date not null,
          qty int not null,
          prc float not null,
 
          primary key (cn, vn, pn)
 
  ) ;

create table test_table(a int, b int );
alter table test_table add column c int;
explain select * from test_table;
alter table test_table drop column c;
alter table test_table add column c int;

set pg_orca.enable_orca to on;

set pg_orca.enable_new_planner TO on;

-- RTE_RESULT
values(1,2);
-- RTE_VALUES
values(1,2),(3,4);
-- values(1,2),(3,4 + 3);

explain verbose select from orders;
explain verbose select * from orders;
explain verbose select o_custkey from orders;
explain verbose select o_custkey, o_custkey from orders;
select n_regionkey as x, n_regionkey as y from nation;

select;

select 1, '1', 2 as x, 'xx' as x;

explain verbose select 1 + 1;

select 1, 1 + 1, true, null, array[1, 2, 3], array[[1], [2], [3]], '[1, 2, 3]';
select 1::text;
select '{1,2,3}'::integer[], 1::text, 1::int, 'a'::text, '99999999'::int;
explain verbose select 1+1 = 3 * 10 and 2 > 1  or 1 is null where 1=1;

explain verbose  select o_orderkey from orders where o_custkey > 10 ;

-- todo, PdxlnRemapOutputColumns
explain verbose select n_regionkey as x, n_regionkey as y from nation where n_regionkey < 10;

explain verbose select 1 where 1 in (2, 3);

explain verbose select n_regionkey as x, n_regionkey  + 1 as y from nation;

explain verbose select 1 from generate_series(1,10);
explain verbose select g from generate_series(1,10) g;
explain verbose select g + 1 from generate_series(1,10) g;
explain verbose select g + 1 as x from generate_series(1,10) g where 1 < 10 ;

explain verbose  select n_regionkey as x, n_regionkey + 1 as y from nation limit 10;
explain verbose select o_totalprice + 1, o_totalprice - 1, o_totalprice * 1, o_totalprice / 1 from orders where o_orderkey = 1000 and o_shippriority + 1 > 10;

-- indexscan
explain verbose select * from nation order by 1 limit 10;
explain verbose select * from nation order by 2 limit 10;
explain verbose select * from nation order by 3 limit 10;
explain verbose select * from nation order by 4 limit 10;

explain verbose select * from orders order by 1 desc limit 10;
explain verbose select * from orders order by 1 desc, 2 asc limit 10;

-- base test cases
explain verbose select o_totalprice + 1 from orders where o_orderkey = 1000 and o_shippriority + 1 < 10;
explain verbose select * from customer where c_custkey in (1,2,2,3,123,34,345,453,56,567,23,213);
explain verbose select * from customer where c_custkey in (1,2,2,3,123,34,345,453,56,567,23,213) or 1+1=2;

explain verbose select sum(n_regionkey) from nation;
explain verbose select sum(n_regionkey) from nation group by n_name;
explain verbose select o_orderkey, sum(o_custkey) from orders group by o_orderkey order by o_orderkey;
explain verbose select o_orderkey, sum(o_custkey) from orders group by o_orderkey order by o_orderkey limit 10;
explain verbose select o_orderkey, sum(o_custkey + o_orderkey) from orders group by o_orderkey order by o_orderkey limit 10;

explain verbose select * from nation UNION select * from nation order by 2 limit 10;
explain verbose select * from nation UNION ALL select * from nation order by 2 limit 10;

explain select * from orders join nation on orders.o_custkey = nation.n_regionkey;
explain select * from orders join nation on orders.o_custkey = nation.n_regionkey and nation.n_regionkey = 1;
explain select * from orders join nation on orders.o_custkey = nation.n_regionkey where nation.n_regionkey = 1;

explain select * from orders left join nation on orders.o_custkey = nation.n_regionkey;
explain select * from orders left join nation on orders.o_custkey = nation.n_regionkey and nation.n_regionkey = 1;
explain select * from orders left join nation on orders.o_custkey = nation.n_regionkey where nation.n_regionkey = 1;

explain select * from nation EXCEPT select * from nation order by 3 limit 10;
explain select * from nation INTERSECT select * from nation order by 3 limit 10;

explain verbose select * from customer where c_custkey in (select c_custkey from nation);
explain verbose select * from customer x where c_custkey in (select x.c_nationkey from nation);

explain verbose select * from customer a join customer b on a.c_custkey != b.c_custkey;

explain verbose select * from customer where customer.c_custkey in (select customer.c_nationkey + 1 from (
 select * from nation where nation.n_nationkey in (select nation.n_nationkey + 1 from orders)
)i);

EXPLAIN verbose with cte as (select * from orders) select * from cte;

EXPLAIN verbose with x as (select o_custkey a1, o_custkey a2, o_custkey from orders) select * from x where a1 = 1 and a2 = 2;

EXPLAIN verbose with cte as (select * from orders left join nation on orders.o_custkey = nation.n_regionkey where nation.n_regionkey = 1)  select * from cte where o_orderkey = 1;

set pg_orca.enable_new_planner TO off;

-- load 'pg_orca.so';
-- set pg_orca.enable_orca to on;

-- set pg_orca.enable_new_planner TO on;


-- explain select * from orders EXCEPT ALL select * from orders order by 6 limit 10;
-- explain select * from orders INTERSECT ALL select * from orders order by 6 limit 10;


-- SemiJoin2InnerJoin
explain verbose select * from customer where exists (select * from nation where c_custkey = n_nationkey and c_nationkey <> n_regionkey);

explain verbose select * from customer where c_custkey in (select distinct c_custkey from nation);
explain verbose select * from customer where c_custkey in (select distinct n_regionkey from nation);
-- explain verbose select *,(select distinct n_regionkey from nation) from customer ;

explain verbose select * from customer where c_custkey in (select c_custkey from nation limit 3);

explain verbose select * from customer where c_custkey > (select sum(n_nationkey) from nation);

explain verbose select * from customer where c_custkey in (select n_nationkey from nation);

EXPLAIN SELECT * FROM customer WHERE 1+c_custkey IN (SELECT c_nationkey+1 FROM nation);

explain verbose select * from customer where 2 in (select n_nationkey + 1 from nation);

explain verbose select * from customer where exists(select n_nationkey from nation where nation.n_nationkey<>customer.c_custkey group by nation.n_nationkey); 

explain verbose SELECT pn, cn, vn FROM sale s WHERE EXISTS (SELECT * FROM customer WHERE EXISTS (SELECT * FROM product WHERE pn = s.pn));

EXPLAIN SELECT pn, cn, vn FROM sale s WHERE cn IN (SELECT s.pn FROM customer WHERE cn NOT IN (SELECT pn FROM product WHERE pn = s.pn));

EXPLAIN select * from customer where exists (select 1 from nation where nation.n_nationkey = customer.c_custkey and customer.c_custkey > 1);

select o_orderkey, sum(o_custkey + o_orderkey)/20 from orders group by o_orderkey order by o_orderkey limit 10;

select * from orders where exists (select 1 from nation where nation.n_regionkey = orders.o_custkey and nation.n_regionkey = 10);
select *, (select 2 from nation where nation.n_regionkey = orders.o_custkey and nation.n_regionkey = 10) from orders where exists (select 1 from nation where nation.n_regionkey = orders.o_custkey and nation.n_regionkey = 10);

-- explain insert into orders select * from orders where o_custkey = 1;
-- explain update orders set o_orderkey = 1 where o_orderkey = 1;
-- explain delete from orders where o_orderkey = 1;

