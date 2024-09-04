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

set pg_orca.enable_orca to on;

load 'pg_orca.so';

explain select * from orders limit 10;

explain select * from orders order by 1 limit 10;
explain select * from orders order by 2 limit 10;
explain select * from orders order by 3 limit 10;
explain select * from orders order by 4 limit 10;
explain select * from orders order by 5 limit 10;

explain select * from orders UNION select * from orders order by 6 limit 10;
explain select * from orders EXCEPT select * from orders order by 6 limit 10;
explain select * from orders INTERSECT select * from orders order by 6 limit 10;

explain select * from orders order by 1 desc limit 10;

explain select * from orders order by 1 asc, 2  limit 10;

-- explain select sum(o_totalprice) from orders;

-- base test cases
explain (verbose) select o_totalprice + 1 from orders where o_orderkey = 1000 and o_shippriority + 1 < 10;

-- SemiJoin2InnerJoin
explain (verbose) select * from customer where exists (select * from nation where c_custkey = n_nationkey and c_nationkey <> n_regionkey);

explain (verbose) select * from customer where c_custkey in (select c_custkey from nation);

explain (verbose) select * from customer where c_custkey in (1,2,2,3,123,34,345,453,56,567,23,213);

explain (verbose) select * from customer where c_custkey in (1,2,2,3,123,34,345,453,56,567,23,213) or 1+1=2;

explain (verbose) select * from customer where c_custkey in (select distinct c_custkey from nation);

explain (verbose) select * from customer where c_custkey in (select c_custkey from nation limit 3);

explain (verbose) select * from customer where c_custkey in (select n_nationkey from nation);

EXPLAIN SELECT * FROM customer WHERE 1+c_custkey IN (SELECT c_nationkey+1 FROM nation);

explain (verbose) select * from customer where 2 in (select n_nationkey + 1 from nation);

explain (verbose) select * from customer where customer.c_custkey in (select customer.c_nationkey + 1 from (
 select * from nation where nation.n_nationkey in (select nation.n_nationkey + 1 from orders)
)i);

explain (verbose) select * from customer a join customer b on a.c_custkey != b.c_custkey;

explain (verbose) select * from customer where exists(select n_nationkey from nation where nation.n_nationkey<>customer.c_custkey group by nation.n_nationkey); 

explain (verbose) SELECT pn, cn, vn FROM sale s WHERE EXISTS (SELECT * FROM customer WHERE EXISTS (SELECT * FROM product WHERE pn = s.pn));

EXPLAIN SELECT pn, cn, vn FROM sale s WHERE cn IN (SELECT s.pn FROM customer WHERE cn NOT IN (SELECT pn FROM product WHERE pn = s.pn));

EXPLAIN select * from customer where exists (select 1 from nation where nation.n_nationkey = customer.c_custkey and customer.c_custkey > 1);

explain verbose select o_custkey,o_orderkey,o_custkey from orders;
explain verbose select o_custkey from orders;
explain verbose select o_orderkey from orders;
explain verbose select * from orders;
explain verbose select o_custkey,o_orderkey from orders;
explain verbose select o_custkey,o_custkey from orders;
explain verbose select o_orderkey,o_custkey from orders;


explain (verbose) select o_totalprice + 1, o_totalprice - 1, o_totalprice * 1, o_totalprice / 1 from orders where o_orderkey = 1000 and o_shippriority + 1 > 10;
explain select o_orderkey from orders limit 10;
explain select * from orders limit 10;
explain select o_orderkey from orders order by 1 limit 10;
explain select * from orders order by 1 limit 10;
explain select o_custkey from orders order by 1 limit 10;
explain select * from orders order by 1 limit 10;


select sum(o_custkey) from orders;
select sum(o_custkey) from orders group by o_orderkey;
select o_orderkey, sum(o_custkey) from orders group by o_orderkey order by o_orderkey;
select o_orderkey, sum(o_custkey) from orders group by o_orderkey order by o_orderkey limit 10;
select o_orderkey, sum(o_custkey + o_orderkey) from orders group by o_orderkey order by o_orderkey limit 10;
-- select o_orderkey, sum(o_custkey + o_orderkey)/20 from orders group by o_orderkey order by o_orderkey limit 10;


select * from orders where exists (select 1 from nation where nation.n_regionkey = orders.o_custkey and nation.n_regionkey = 10);

explain insert  into orders select * from orders where o_custkey = 1;
explain update orders set o_orderkey = 1 where o_orderkey = 1;
explain delete from orders where o_orderkey = 1;

select * from orders join nation on orders.o_custkey = nation.n_regionkey;
select * from orders join nation on orders.o_custkey = nation.n_regionkey and nation.n_regionkey = 1;
select * from orders join nation on orders.o_custkey = nation.n_regionkey where nation.n_regionkey = 1;

explain select * from orders left join nation on orders.o_custkey = nation.n_regionkey;
explain select * from orders left join nation on orders.o_custkey = nation.n_regionkey and nation.n_regionkey = 1;
explain select * from orders left join nation on orders.o_custkey = nation.n_regionkey where nation.n_regionkey = 1;


EXPLAIN with cte as (select * from orders) select * from cte;

EXPLAIN with x as (select o_custkey a1, o_custkey a2, o_custkey from orders) select * from x where a1 = 1 and a2 = 2;

EXPLAIN with cte as (select * from orders left join nation on orders.o_custkey = nation.n_regionkey where nation.n_regionkey = 1)  select * from cte where o_orderkey = 1;

set pg_orca.enable_orca to off;
select query as query1 from tpch_queries(1); \gset
select query as query2 from tpch_queries(2); \gset
select query as query3 from tpch_queries(3); \gset
select query as query4 from tpch_queries(4); \gset
select query as query5 from tpch_queries(5); \gset
select query as query6 from tpch_queries(6); \gset
select query as query7 from tpch_queries(7); \gset
select query as query8 from tpch_queries(8); \gset
select query as query9 from tpch_queries(9); \gset
select query as query10 from tpch_queries(10); \gset
select query as query11 from tpch_queries(11); \gset
select query as query12 from tpch_queries(12); \gset
select query as query13 from tpch_queries(13); \gset
select query as query14 from tpch_queries(14); \gset
select query as query15 from tpch_queries(15); \gset
select query as query16 from tpch_queries(16); \gset
select query as query17 from tpch_queries(17); \gset
select query as query18 from tpch_queries(18); \gset
select query as query19 from tpch_queries(19); \gset
select query as query20 from tpch_queries(20); \gset
select query as query21 from tpch_queries(21); \gset
select query as query22 from tpch_queries(22); \gset
set pg_orca.enable_orca to on;

explain (costs off ) :query1;
:query1;

explain (costs off ) :query2;
:query2;

explain (costs off ) :query3;
:query3;

explain (costs off ) :query4;
:query4;

explain (costs off ) :query5;
:query5;

explain (costs off ) :query6;
:query6;

explain (costs off ) :query7;
:query7;

-- explain (costs off ) :query8;
-- :query8;

explain (costs off ) :query9;
:query9;

explain (costs off ) :query10;
:query10;

explain (costs off ) :query11;
:query11;

explain (costs off ) :query12;
:query12;

explain (costs off ) :query13;
:query13;

-- explain (costs off ) :query14;
-- :query14;

explain (costs off ) :query15;
:query15;

explain (costs off ) :query16;
:query16;

-- explain (costs off ) :query17;
-- :query17;

explain (costs off ) :query18;
:query18;

explain (costs off ) :query19;
:query19;

-- explain (costs off ) :query20;
-- :query20;

explain (costs off ) :query21;
:query21;

explain (costs off ) :query22;
:query22;


select 1 where 1 in (2, 3);

select 1;

select 1 from generate_series(1,10);