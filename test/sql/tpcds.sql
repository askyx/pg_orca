set pg_orca.enable_orca to off;

create database tpcds;
\c tpcds

create extension pg_tpcds;

set pg_orca.enable_orca to off;
select query as query1 from tpcds_queries(1); \gset
select query as query2 from tpcds_queries(2); \gset
select query as query3 from tpcds_queries(3); \gset
select query as query4 from tpcds_queries(4); \gset
select query as query5 from tpcds_queries(5); \gset
select query as query6 from tpcds_queries(6); \gset
select query as query7 from tpcds_queries(7); \gset
select query as query8 from tpcds_queries(8); \gset
select query as query9 from tpcds_queries(9); \gset

select query as query10 from tpcds_queries(10); \gset
select query as query11 from tpcds_queries(11); \gset
select query as query12 from tpcds_queries(12); \gset
select query as query13 from tpcds_queries(13); \gset
select query as query14 from tpcds_queries(14); \gset
select query as query15 from tpcds_queries(15); \gset
select query as query16 from tpcds_queries(16); \gset
select query as query17 from tpcds_queries(17); \gset
select query as query18 from tpcds_queries(18); \gset
select query as query19 from tpcds_queries(19); \gset

select query as query20 from tpcds_queries(20); \gset
select query as query21 from tpcds_queries(21); \gset
select query as query22 from tpcds_queries(22); \gset
select query as query23 from tpcds_queries(23); \gset
select query as query24 from tpcds_queries(24); \gset
select query as query25 from tpcds_queries(25); \gset
select query as query26 from tpcds_queries(26); \gset
select query as query27 from tpcds_queries(27); \gset
select query as query28 from tpcds_queries(28); \gset
select query as query29 from tpcds_queries(29); \gset

select query as query30 from tpcds_queries(30); \gset
select query as query31 from tpcds_queries(31); \gset
select query as query32 from tpcds_queries(32); \gset
select query as query33 from tpcds_queries(33); \gset
select query as query34 from tpcds_queries(34); \gset
select query as query35 from tpcds_queries(35); \gset
select query as query36 from tpcds_queries(36); \gset
select query as query37 from tpcds_queries(37); \gset
select query as query38 from tpcds_queries(38); \gset
select query as query39 from tpcds_queries(39); \gset

select query as query40 from tpcds_queries(40); \gset
select query as query41 from tpcds_queries(41); \gset
select query as query42 from tpcds_queries(42); \gset
select query as query43 from tpcds_queries(43); \gset
select query as query44 from tpcds_queries(44); \gset
select query as query45 from tpcds_queries(45); \gset
select query as query46 from tpcds_queries(46); \gset
select query as query47 from tpcds_queries(47); \gset
select query as query48 from tpcds_queries(48); \gset
select query as query49 from tpcds_queries(49); \gset

select query as query50 from tpcds_queries(50); \gset
select query as query51 from tpcds_queries(51); \gset
select query as query52 from tpcds_queries(52); \gset
select query as query53 from tpcds_queries(53); \gset
select query as query54 from tpcds_queries(54); \gset
select query as query55 from tpcds_queries(55); \gset
select query as query56 from tpcds_queries(56); \gset
select query as query57 from tpcds_queries(57); \gset
select query as query58 from tpcds_queries(58); \gset
select query as query59 from tpcds_queries(59); \gset

select query as query60 from tpcds_queries(60); \gset
select query as query61 from tpcds_queries(61); \gset
select query as query62 from tpcds_queries(62); \gset
select query as query63 from tpcds_queries(63); \gset
select query as query64 from tpcds_queries(64); \gset
select query as query65 from tpcds_queries(65); \gset
select query as query66 from tpcds_queries(66); \gset
select query as query67 from tpcds_queries(67); \gset
select query as query68 from tpcds_queries(68); \gset
select query as query69 from tpcds_queries(69); \gset

select query as query70 from tpcds_queries(70); \gset
select query as query71 from tpcds_queries(71); \gset
select query as query72 from tpcds_queries(72); \gset
select query as query73 from tpcds_queries(73); \gset
select query as query74 from tpcds_queries(74); \gset
select query as query75 from tpcds_queries(75); \gset
select query as query76 from tpcds_queries(76); \gset
select query as query77 from tpcds_queries(77); \gset
select query as query78 from tpcds_queries(78); \gset
select query as query79 from tpcds_queries(79); \gset

select query as query80 from tpcds_queries(80); \gset
select query as query81 from tpcds_queries(81); \gset
select query as query82 from tpcds_queries(82); \gset
select query as query83 from tpcds_queries(83); \gset
select query as query84 from tpcds_queries(84); \gset
select query as query85 from tpcds_queries(85); \gset
select query as query86 from tpcds_queries(86); \gset
select query as query87 from tpcds_queries(87); \gset
select query as query88 from tpcds_queries(88); \gset
select query as query89 from tpcds_queries(89); \gset

select query as query90 from tpcds_queries(90); \gset
select query as query91 from tpcds_queries(91); \gset
select query as query92 from tpcds_queries(92); \gset
select query as query93 from tpcds_queries(93); \gset
select query as query94 from tpcds_queries(94); \gset
select query as query95 from tpcds_queries(95); \gset
select query as query96 from tpcds_queries(96); \gset
select query as query97 from tpcds_queries(97); \gset
select query as query98 from tpcds_queries(98); \gset
select query as query99 from tpcds_queries(99); \gset
set pg_orca.enable_orca to on;

-- explain (costs off ) :query1;
-- explain (costs off ) :query2;
explain (costs off ) :query3;
-- explain (costs off ) :query4;
-- explain (costs off ) :query5;
-- explain (costs off ) :query6;
explain (costs off ) :query7;
-- explain (costs off ) :query8;
explain (costs off ) :query9;

explain (costs off ) :query10;
-- explain (costs off ) :query11;
-- explain (costs off ) :query12;
explain (costs off ) :query13;
-- explain (costs off ) :query14;
explain (costs off ) :query15;
explain (costs off ) :query16;
-- explain (costs off ) :query17;
-- explain (costs off ) :query18;
explain (costs off ) :query19;

-- explain (costs off ) :query20;
explain (costs off ) :query21;
-- explain (costs off ) :query22;
-- explain (costs off ) :query23;
-- explain (costs off ) :query24;
explain (costs off ) :query25;
explain (costs off ) :query26;
-- explain (costs off ) :query27;
explain (costs off ) :query28;
explain (costs off ) :query29;


-- explain (costs off ) :query30;
-- explain (costs off ) :query31;
-- explain (costs off ) :query32;
-- explain (costs off ) :query33;
-- explain (costs off ) :query34;
-- explain (costs off ) :query35;
-- explain (costs off ) :query36;
-- explain (costs off ) :query37;
-- explain (costs off ) :query38;
-- explain (costs off ) :query39;

-- explain (costs off ) :query40;
-- explain (costs off ) :query41;
-- explain (costs off ) :query42;
-- explain (costs off ) :query43;
-- explain (costs off ) :query44;
-- explain (costs off ) :query45;
-- explain (costs off ) :query46;
-- explain (costs off ) :query47;
-- explain (costs off ) :query48;
-- explain (costs off ) :query49;

-- explain (costs off ) :query50;
-- explain (costs off ) :query51;
-- explain (costs off ) :query52;
-- explain (costs off ) :query53;
-- explain (costs off ) :query54;
-- explain (costs off ) :query55;
-- explain (costs off ) :query56;
-- explain (costs off ) :query57;
-- explain (costs off ) :query58;
-- explain (costs off ) :query59;

-- explain (costs off ) :query60;
-- explain (costs off ) :query61;
-- explain (costs off ) :query62;
-- explain (costs off ) :query63;
-- explain (costs off ) :query64;
-- explain (costs off ) :query65;
-- explain (costs off ) :query66;
-- explain (costs off ) :query67;
-- explain (costs off ) :query68;
-- explain (costs off ) :query69;

-- explain (costs off ) :query70;
-- explain (costs off ) :query71;
-- explain (costs off ) :query72;
-- explain (costs off ) :query73;
-- explain (costs off ) :query74;
-- explain (costs off ) :query75;
-- explain (costs off ) :query76;
-- explain (costs off ) :query77;
-- explain (costs off ) :query78;
-- explain (costs off ) :query79;

-- explain (costs off ) :query80;
-- explain (costs off ) :query81;
-- explain (costs off ) :query82;
-- explain (costs off ) :query83;
-- explain (costs off ) :query84;
-- explain (costs off ) :query85;
-- explain (costs off ) :query86;
-- explain (costs off ) :query87;
-- explain (costs off ) :query88;
-- explain (costs off ) :query89;

-- explain (costs off ) :query90;
-- explain (costs off ) :query91;
-- explain (costs off ) :query92;
-- explain (costs off ) :query93;
-- explain (costs off ) :query94;
-- explain (costs off ) :query95;
-- explain (costs off ) :query96;
-- explain (costs off ) :query97;
-- explain (costs off ) :query98;
-- explain (costs off ) :query99;