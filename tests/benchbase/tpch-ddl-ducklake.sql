DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;

CREATE TABLE region (
    r_regionkey integer,
    r_name      char(25),
    r_comment   varchar(152)
);

CREATE TABLE nation (
    n_nationkey integer,
    n_name      char(25),
    n_regionkey integer,
    n_comment   varchar(152)
);

CREATE TABLE part (
    p_partkey     integer,
    p_name        varchar(55),
    p_mfgr        char(25),
    p_brand       char(10),
    p_type        varchar(25),
    p_size        integer,
    p_container   char(10),
    p_retailprice double,
    p_comment     varchar(23)
);

CREATE TABLE supplier (
    s_suppkey   integer,
    s_name      char(25),
    s_address   varchar(40),
    s_nationkey integer,
    s_phone     char(15),
    s_acctbal   double,
    s_comment   varchar(101)
);

CREATE TABLE partsupp (
    ps_partkey    integer,
    ps_suppkey    integer,
    ps_availqty   integer,
    ps_supplycost double,
    ps_comment    varchar(199)
);

CREATE TABLE customer (
    c_custkey    integer,
    c_name       varchar(25),
    c_address    varchar(40),
    c_nationkey  integer,
    c_phone      char(15),
    c_acctbal    double,
    c_mktsegment char(10),
    c_comment    varchar(117)
);

CREATE TABLE orders (
    o_orderkey      integer,
    o_custkey       integer,
    o_orderstatus   char(1),
    o_totalprice    double,
    o_orderdate     date,
    o_orderpriority char(15),
    o_clerk         char(15),
    o_shippriority  integer,
    o_comment       varchar(79)
);

CREATE TABLE lineitem (
    l_orderkey      integer,
    l_partkey       integer,
    l_suppkey       integer,
    l_linenumber    integer,
    l_quantity      double,
    l_extendedprice double,
    l_discount      double,
    l_tax           double,
    l_returnflag    char(1),
    l_linestatus    char(1),
    l_shipdate      date,
    l_commitdate    date,
    l_receiptdate   date,
    l_shipinstruct  char(25),
    l_shipmode      char(10),
    l_comment       varchar(44)
);
