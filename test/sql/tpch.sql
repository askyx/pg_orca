set pg_orca.enable_orca to off;

create database tpch;
\c tpch

create extension pg_tpch;

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

