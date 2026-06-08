package io.debezium.connector.yashandb.antlr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.YashanDBConnectorConfig;
import io.debezium.connector.yashandb.YashanDBValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

import junit.framework.TestCase;

public class YashanDBDdlParserTest extends TestCase {

    public void testParse() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse("CREATE TABLE \"HSYS\".\"CUSTOMER_READING_ROUTE\"\n" +
                "(\"READING_ROUTE_ID\" BIGINT NOT NULL ENABLE,\n" +
                "\"REVISION\" INTEGER DEFAULT 0,\n" +
                "\"CREATED_BY\" BIGINT NOT NULL ENABLE,\n" +
                "\"CREATED_TIME\" TIMESTAMP(6) NOT NULL ENABLE,\n" +
                "\"UPDATED_BY\" BIGINT,\n" +
                "\"UPDATED_TIME\" TIMESTAMP(6),\n" +
                "\"DEPT_ID\" BIGINT NOT NULL ENABLE,\n" +
                "\"READING_ROUTE_CODE\" VARCHAR(60) NOT NULL ENABLE,\n" +
                "\"READING_ROUTE_NAME\" VARCHAR(150),\n" +
                "\"REMARK\" VARCHAR(768),\n" +
                "CONSTRAINT \"SYS_C_19536\" PRIMARY KEY (\"READING_ROUTE_ID\")\n" +
                "USING INDEX\n" +
                "PCTFREE 8 INITRANS 2 MAXTRANS 255\n" +
                "TABLESPACE \"USERS\" ENABLE\n" +
                ");", databaseTables);
        ddlParser.parse("alter table \"HSYS\".\"CUSTOMER_READING_ROUTE\" add column id01 NUMBER;", databaseTables);
        Table table = databaseTables.forTable(new TableId(null, null, "CUSTOMER_READING_ROUTE"));
        Column id01 = table.columnWithName("ID01");
        assert Objects.equals(id01.typeName(), "NUMBER");

        ddlParser.parse("CREATE TABLE \"HSYS\".\"CUSTOMER_READING_ROUTE1111\"\n" +
                "(\"READING_ROUTE_ID\" BIGINT NOT NULL ENABLE,\n" +
                "\"REVISION\" INTEGER DEFAULT 0,\n" +
                "\"CREATED_BY\" BIGINT NOT NULL ENABLE,\n" +
                "\"CREATED_TIME\" TIMESTAMP(6) NOT NULL ENABLE,\n" +
                "\"UPDATED_BY\" BIGINT,\n" +
                "\"UPDATED_TIME\" TIMESTAMP(6),\n" +
                "\"DEPT_ID\" BIGINT NOT NULL ENABLE,\n" +
                "\"READING_ROUTE_CODE\" NVARCHAR(40) NOT NULL ENABLE,\n" +
                "\"READING_ROUTE_NAME\" NVARCHAR(100),\n" +
                "\"REMARK\" NVARCHAR(512),\n" +
                "PRIMARY KEY (\"READING_ROUTE_ID\")\n" +
                "USING INDEX\n" +
                "PCTFREE 8 INITRANS 2 MAXTRANS 255\n" +
                "TABLESPACE \"USERS\" ENABLE\n" +
                ") PCTFREE 8 INITRANS 2 MAXTRANS 255\n" +
                "LOGGING\n" +
                "TABLESPACE \"USERS\"\n" +
                "SEGMENT CREATION DEFERRED\n" +
                "ORGANIZATION HEAP;", databaseTables);
        ddlParser.parse("ALTER TABLE HSYS.CUSTOMER_READING_ROUTE1111 MODIFY READING_ROUTE_CODE NVARCHAR(10);", databaseTables);
        ddlParser.parse("ALTER TABLE HSYS.CUSTOMER_READING_ROUTE1111 MODIFY REMARK NVARCHAR(512);", databaseTables);
        ddlParser.parse("ALTER TABLE HSYS.CUSTOMER_READING_ROUTE1111 MODIFY READING_ROUTE_NAME NVARCHAR(100);", databaseTables);
        assert databaseTables.forTable(new TableId(null, null, "CUSTOMER_READING_ROUTE1111")) != null;
        assert Objects.equals(databaseTables.forTable(new TableId(null, null, "CUSTOMER_READING_ROUTE1111"))
                .columnWithName("READING_ROUTE_CODE").typeName(), "NVARCHAR2");
    }

    @Test
    public void testTime() {
        String sql = "create table TEST_TIME_DB.TIME_TAB01(id int,id01 time)";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println();
    }

    @Test
    public void testTableNameIsEnd() {
        String sql = "create table TEST_TIME_DB.END(id int,id01 time)";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println();
    }

    @Test
    public void testDropMutipleColumn() {
        String sql = "ALTER TABLE DDL_CREATE.product_pri DROP COLUMN(product_no,product_name);";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println();
    }

    @Test
    public void testHashListTable() {
        String sql = "CREATE TABLE DDL_CREATE.hash_list_table(c1 INT, c2 VARCHAR(10))\n" +
                "PARTITION BY HASH(c1)\n" +
                "SUBPARTITION BY LIST(c2)\n" +
                "(\n" +
                "PARTITION p1(SUBPARTITION sp1 VALUES('a')), \n" +
                "PARTITION p2 (SUBPARTITION sp3 VALUES('d'), SUBPARTITION sp4 VALUES(DEFAULT))\n" +
                ");";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println();
    }

    @Test
    public void testDropPrimaryKey() {
        String sql = "ALTER TABLE DDL_CREATE.department DROP PRIMARY KEY;";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println();
    }

    @Test
    public void testPartitionByHas() {
        String sql = "CREATE TABLE DDL_CREATE.sales_info\n" +
                "(year CHAR(4) NOT NULL,\n" +
                " month CHAR(2) NOT NULL,\n" +
                " branch CHAR(4) CONSTRAINT c_sales_info_1 REFERENCES DDL_CREATE.branches(branch_no) ON DELETE SET NULL,\n" +
                " quantity NUMBER DEFAULT 0 NOT NULL,\n" +
                " amount NUMBER(10,2) DEFAULT 0 NOT NULL)\n" +
                "PARTITION BY HASH(branch)\n" +
                "SUBPARTITION BY LIST(year)\n" +
                "SUBPARTITION template (SUBPARTITION sp_sales_info_1 VALUES ('2001','2002','2010'),\n" +
                " SUBPARTITION sp_sales_info_2 VALUES ('2021','2020','2019'),\n" +
                " SUBPARTITION sp_sales_info_3 VALUES (DEFAULT))\n" +
                "(PARTITION p_sales_info_1,PARTITION p_sales_info_2,PARTITION p_sales_info_3);";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println();
    }

    @Test
    public void testAlterPartition() {
        String sql = "ALTER TABLE DDL_CREATE.composite_table MODIFY PARTITION p1 ADD SUBPARTITION p1_subp1";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println(databaseTables);
    }

    @Test
    public void testDdl() {
        String sql = "create table KAFKA_DDL.tab(id int) ORGANIZATION external (type YASDB_LOADER  access parameters (RECORDS DELIMITED BY NEWLINE));";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println(databaseTables);
    }

    @Test
    public void testDdlPartition() {
        String sql = "CREATE TABLE orders_multikey(\n" +
                "order_no CHAR(14) NOT NULL,\n" +
                "order_desc VARCHAR2(100),\n" +
                "area CHAR(2),\n" +
                "branch CHAR(4),\n" +
                "order_date DATE DEFAULT SYSDATE NOT NULL,\n" +
                "salesperson CHAR(10),\n" +
                "id NUMBER)\n" +
                "PARTITION BY RANGE(id,order_date)\n" +
                "(PARTITION p_orders_max_1 VALUES LESS THAN (800,'2010-01-01'),\n" +
                " PARTITION p_orders_max_2 VALUES LESS THAN (1000,'2010-05-01'),\n" +
                " PARTITION p_orders_max_3 VALUES LESS THAN (1500,'2010-10-01')\n" +
                ");";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println(databaseTables);
    }

    @Test
    public void testDdlSYSDate() {
        String sql = "CREATE TABLE xx2(id time DEFAULT sysdate);";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println(databaseTables);
    }

    @Test
    public void testDdl3() {
        String sql = "create table test_default_beyond(\n" +
                "  col1 BIGINT default 9223372036854775807,\n" +
                "  col2 varchar(10) default 'abchd~!@#$',\n" +
                "  col3 char(10) default 'abchd~!@#$',\n" +
                "  col4 float default 3.402823E38,\n" +
                "  col5 double default 1.79769313486231E308,\n" +
                "  col6 number(38,0) default 99999999999999999999999999999999999999,\n" +
                "  col7 timestamp(6)  default '9999-12-31 23:59:59.999999',\n" +
                "  col8 timestamp not null default '1-1-1 00:00:00.000000',\n" +
                "  col9 timestamp NOT NULL default current_timestamp,\n" +
                "  col10 time default '23:59:59.999999',\n" +
                "  col11 time default '00:00:00.000000',\n" +
                "  col12 timestamp(9) default '9999-12-31 23:59:59.999999',\n" +
                "  col13 date default '9999-12-31',\n" +
                "  col14 boolean default FALSE\n" +
                ");";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println(databaseTables);
    }

    public void testExternalTable() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // access_driver_type
        String sql = "create table KAFKA_DDL.location_specifier(id int) ORGANIZATION\n" +
                " EXTERNAL (TYPE YASDB_LOADER) ";
        ddlParser.parse(sql, databaseTables);
        String sql2 = "create table KAFKA_DDL.location_specifier2(id int) ORGANIZATION\n" +
                " EXTERNAL (TYPE ORACLE_LOADER) ";

        ddlParser.parse(sql2, databaseTables);
        // external_table_data_props_clause
        // record_format_info_clause
        String sql3 = "create table KAFKA_DDL.record_format_info_clause1(id int) ORGANIZATION external" +
                " (  access parameters (RECORDS DELIMITED BY NEWLINE));";
        ddlParser.parse(sql3, databaseTables);
        // output_files_clause
        String sql13 = "create table KAFKA_DDL.record_format_info_clause2(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS NOBADFILE))";
        ddlParser.parse(sql13, databaseTables);

        String sql23 = "create table KAFKA_DDL.record_format_info_clause3(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS BADFILE test:'./'));";
        ddlParser.parse(sql23, databaseTables);

        String sql33 = "create table KAFKA_DDL.record_format_info_clause4(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS NOLOGFILE));";
        ddlParser.parse(sql33, databaseTables);

        String sql43 = "create table KAFKA_DDL.record_format_info_clause5(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS LOGFILE test:'./'));";
        ddlParser.parse(sql43, databaseTables);
        // field_definitions_clause
        String sql4 = "create table KAFKA_DDL.tab12(id int) ORGANIZATION " +
                "external (type YASDB_LOADER default directory test access parameters (RECORDS DELIMITED BY NEWLINE));";

        ddlParser.parse(sql4, databaseTables);

        String sql14 = "create table KAFKA_DDL.field_definitions_clause1(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY 'char'));";

        ddlParser.parse(sql14, databaseTables);

        String sql24 = "create table KAFKA_DDL.field_definitions_clause2(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS OPTIONALLY ENCLOSED BY 'char'));";

        ddlParser.parse(sql24, databaseTables);

        String sql34 = "create table KAFKA_DDL.field_definitions_clause3(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS));";

        ddlParser.parse(sql34, databaseTables);

        String sql44 = "create table KAFKA_DDL.field_definitions_clause4(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY 0x'9'));";

        ddlParser.parse(sql44, databaseTables);

        String sql54 = "create table KAFKA_DDL.field_definitions_clause5(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY x'9'));";

        ddlParser.parse(sql54, databaseTables);
        // LOCATION
        String sql542 = "create table KAFKA_DDL.location_specifier(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY x'9') LOCATION (test:'./test.csv'));";

        ddlParser.parse(sql542, databaseTables);
        // REJECT
        String reject1 = "create table KAFKA_DDL.REJECT1(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY x'9') LOCATION (test:'./test.csv')) REJECT LIMIT 1;";

        ddlParser.parse(reject1, databaseTables);

        String reject2 = "create table KAFKA_DDL.REJECT1(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY x'9') LOCATION (test:'./test.csv')) REJECT LIMIT UNLIMITED;";

        ddlParser.parse(reject2, databaseTables);
        System.out.println(databaseTables);
    }

    public void testLscProperties() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // access_driver_type
        String sql = "      CREATE TABLE finance_info\n" +
                "(year CHAR(4) ,\n" +
                "month CHAR(2) ,\n" +
                "branch CHAR(4) ,\n" +
                "revenue_total NUMBER(10,2),\n" +
                "cost_total NUMBER(10,2),\n" +
                "fee_total NUMBER(10,2)\n" +
                ") ORGANIZATION LSC\n" +
                "COMPRESSION lz4 HIGH\n" +
                "ORDER BY (year,month,branch)\n" +
                "MCOL TTL '1' MONTH;";
        ddlParser.parse(sql, databaseTables);
        System.out.println(databaseTables);
    }

    public void testTableType() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table type
        String sql = "create global temporary table tem_tab(id int );";
        ddlParser.parse(sql, databaseTables);
        String sql1 = "CREATE SHARDED TABLE area_shard\n" +
                "(area_no CHAR(2) NOT NULL,\n" +
                "area_name VARCHAR2(60),\n" +
                "DHQ VARCHAR2(20) DEFAULT 'ShenZhen' NOT NULL);";
        ddlParser.parse(sql1, databaseTables);
        String sql2 = "     CREATE DUPLICATED TABLE area_dupli\n" +
                "(area_no CHAR(2) NOT NULL,\n" +
                "area_name VARCHAR2(60),\n" +
                "DHQ VARCHAR2(20) DEFAULT 'ShenZhen' NOT NULL);";
        ddlParser.parse(sql2, databaseTables);
        System.out.println(databaseTables);
    }

    public void testTempTableAttrClause() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties temp_table_attr_clause
        String sql = "create table attr_tab(id int) on commit  drop definition;";
        ddlParser.parse(sql, databaseTables);
        String sql1 = "create table attr_tab2(id int) on commit preserve definition;";
        ddlParser.parse(sql1, databaseTables);
        String sql2 = "create table attr_tab3(id int) on commit preserve rows;";
        ddlParser.parse(sql2, databaseTables);
        String sql3 = "create table attr_tab4(id int) on commit delete rows;";
        ddlParser.parse(sql3, databaseTables);
        System.out.println(databaseTables);
    }

    public void testOrganizationClause() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties organization_clause
        String sql = "create table organ_heap(id int)organization heap;";
        ddlParser.parse(sql, databaseTables);
        String sql1 = "create table organ_heap1(id int)organization tac;";
        ddlParser.parse(sql1, databaseTables);
        String sql2 = "create table organ_heap2(id int)organization lsc;";
        ddlParser.parse(sql2, databaseTables);
        System.out.println(databaseTables);
    }

    public void testExternalTableClause() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties external_table_clause
        String sql = "create table organ_heap(id int)organization EXTERNAL (type YASDB_LOADER);";
        ddlParser.parse(sql, databaseTables);
        String sql1 = "create table organ_heap2(id int)organization EXTERNAL (type ORACLE_LOADER);";
        ddlParser.parse(sql1, databaseTables);
        System.out.println(databaseTables);
    }

    public void testExternalTableClause2() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties external_table_clause
        String sql = "CREATE TABLE DDL_CREATE.orders_info1 (order_no CHAR(14) NOT NULL,\n" +
                "order_desc VARCHAR2(100),\n" +
                "area CHAR(2),\n" +
                "branch CHAR(4),\n" +
                "order_date DATE DEFAULT SYSDATE NOT NULL,\n" +
                "salesperson CHAR(10),\n" +
                "id NUMBER)\n" +
                "PARTITION BY RANGE (order_date)\n" +
                "INTERVAL (INTERVAL '3' month) STORE IN (mu_data_001)\n" +
                "(PARTITION p_firstquarter VALUES LESS THAN (DATE '2000-01-01'));";
        ddlParser.parse(sql, databaseTables);
        System.out.println(databaseTables);
    }

    public void testExternalTableClause3() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties external_table_clause
        String sql = "CREATE TABLE sales_info\n" +
                "(year CHAR(4) NOT NULL,\n" +
                "month CHAR(2) NOT NULL,\n" +
                "branch CHAR(4) NOT NULL,\n" +
                "product CHAR(10) NOT NULL,\n" +
                "quantity NUMBER DEFAULT 0 NOT NULL,\n" +
                "amount NUMBER(10,2) DEFAULT 0 NOT NULL,\n" +
                "salsperson CHAR(10))\n" +
                "PARTITION BY LIST(year,month)\n" +
                "(PARTITION p_sales_info1_1 VALUES (('2018','01'),('2018','02')),\n" +
                "PARTITION p_sales_info1_2 VALUES ('2020','01'),\n" +
                "PARTITION p_sales_info1_3 VALUES (DEFAULT));";
        ddlParser.parse(sql, databaseTables);
        String sql1 = " CREATE TABLE DDL_CREATE.orders_info1 (order_no CHAR(14) NOT NULL,\n" +
                "order_desc VARCHAR2(100),\n" +
                "area CHAR(2),\n" +
                "branch CHAR(4),\n" +
                "order_date DATE DEFAULT SYSDATE NOT NULL,\n" +
                "salesperson CHAR(10),\n" +
                "id NUMBER)\n" +
                "PARTITION BY RANGE (order_date)\n" +
                "INTERVAL (INTERVAL '3' month) STORE IN (mu_data_001)\n" +
                "(PARTITION p_firstquarter VALUES LESS THAN (DATE '2000-01-01'));";
        ddlParser.parse(sql1, databaseTables);
        String sql2 = " CREATE TABLE DDL_CREATE.hh_composite(a INT, b VARCHAR(10))\n" +
                "PARTITION BY HASH(a)\n" +
                "SUBPARTITION BY HASH(b)\n" +
                "subpartitions 8\n" +
                "(PARTITION p1, PARTITION p2);";
        ddlParser.parse(sql2, databaseTables);
        String sql3 = "CREATE TABLE DDL_CREATE.hr_composite(a INT, b INT, c INT,d INT)\n" +
                "PARTITION BY HASH(a)\n" +
                "SUBPARTITION BY RANGE(b,c,d)\n" +
                "(\n" +
                "PARTITION p1 (SUBPARTITION sp1 VALUES LESS than(10,20,30), SUBPARTITION sp2 VALUES LESS than(20,20,30)),\n" +
                "PARTITION p2\n" +
                ");";
        ddlParser.parse(sql3, databaseTables);
        String sql4 = "CREATE TABLE DDL_CREATE.hr_composite_template(a INT, b VARCHAR(10))\n" +
                "PARTITION BY HASH(a)\n" +
                "SUBPARTITION BY RANGE(b)\n" +
                "SUBPARTITION template (SUBPARTITION sub1 VALUES LESS than ('a') , SUBPARTITION sub2 VALUES LESS than (MAXVALUE))\n" +
                "(PARTITION p1,PARTITION p2);";
        ddlParser.parse(sql4, databaseTables);
        String sql5 = "CREATE TABLE DDL_CREATE.list_composite(a INT, b VARCHAR(10)) \n" +
                "PARTITION BY HASH(a) SUBPARTITION BY LIST(b)\n" +
                "SUBPARTITION template(SUBPARTITION sp1 VALUES('a'), SUBPARTITION sp2 VALUES(DEFAULT))\n" +
                "partitions 8;";
        ddlParser.parse(sql5, databaseTables);
        String sql6 = "CREATE TABLE DDL_CREATE.hash_composite(a INT, b VARCHAR(10))\n" +
                "PARTITION BY HASH(a)\n" +
                "SUBPARTITION BY HASH(b)\n" +
                "SUBPARTITION template (SUBPARTITION sp1 TABLESPACE users, SUBPARTITION sp2 TABLESPACE users)\n" +
                "(PARTITION p1, PARTITION p2);";
        ddlParser.parse(sql6, databaseTables);
        String sql7 = "CREATE TABLE DDL_CREATE.hl_partcomposite1(c1 INT,c2 INT)\n" +
                "PARTITION BY HASH(c1)\n" +
                "SUBPARTITION BY LIST(c2)\n" +
                "SUBPARTITION template(SUBPARTITION sp9 VALUES(2))\n" +
                "(PARTITION p1 (SUBPARTITION sp11 VALUES(1)),\n" +
                "PARTITION p2,\n" +
                "PARTITION p3);";
        ddlParser.parse(sql7, databaseTables);
        System.out.println(databaseTables);
    }

    public void testPhysicalAttributeClause() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties physical_attribute_clause
        String sql = "create table physical_attribute_tab(id int)tablespace users;";
        ddlParser.parse(sql, databaseTables);
        String sql1 = "create table physical_attribute_tab1(id int)tablespace set users;";
        ddlParser.parse(sql1, databaseTables);
        String sql2 = "create table physical_attribute_tab2(id int)PCTFREE 1;";
        ddlParser.parse(sql2, databaseTables);
        String sql3 = "create table physical_attribute_tab3(id int)PCTUSED 1;";
        ddlParser.parse(sql3, databaseTables);

        String sql4 = "create table physical_attribute_tab4(id int)INITRANS 1;";
        ddlParser.parse(sql4, databaseTables);
        String sql5 = "create table physical_attribute_tab5(id int)MAXTRANS 1;";
        ddlParser.parse(sql5, databaseTables);
        String sql6 = "create table physical_attribute_tab15(id int)segment creation immediate;";
        ddlParser.parse(sql6, databaseTables);

        String sql7 = "create table physical_attribute_tab25(id int)segment creation deferred;";
        ddlParser.parse(sql7, databaseTables);
        String sql8 = "CREATE TABLE part_storage1(a INT, b VARCHAR(4000))\n" +
                "PARTITION BY RANGE(a, b)\n" +
                "(\n" +
                "\tPARTITION p1 VALUES LESS THAN(1, 'a') STORAGE(INITIAL 0 MAXSIZE 1M NEXT 0),\n" +
                "\tPARTITION p2 VALUES LESS THAN(10, 'c') STORAGE(MINEXTENTS 1 MAXEXTENTS 10 PCTINCREASE 0),\n" +
                "\tPARTITION p3 VALUES LESS THAN(MAXVALUE, MAXVALUE) STORAGE(INITIAL 0 MAXSIZE 1M NEXT 0 MINEXTENTS 1 MAXEXTENTS 10 PCTINCREASE 0 FREELIST GROUPS 20 BUFFER_POOL RECYCLE FLASH_CACHE KEEP CELL_FLASH_CACHE DEFAULT)\n"
                +
                ")\n" +
                "STORAGE(INITIAL 63K MAXSIZE 10M NEXT 12k MINEXTENTS 1 MAXEXTENTS 10 PCTINCREASE 0 FREELISTS 10);";
        ddlParser.parse(sql8, databaseTables);
        System.out.println(databaseTables);
    }

    public void testSqlFile() throws IOException {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties external_table_clause
        List<String> strings = parseSqlFile("src/test/resources/rename.sql");
        strings.forEach(s -> {
            ddlParser.parse(s, databaseTables);
        });
        System.out.println(databaseTables);
    }

    public void testSqlFile2() throws IOException {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null), Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties external_table_clause
        List<String> strings = parseSqlFile("src/test/resources/约束.sql");
        strings.forEach(s -> {
            ddlParser.parse(s, databaseTables);
        });
        System.out.println(databaseTables);
    }

    public List<String> parseSqlFile(String filePath) throws IOException {
        List<String> sqlList = new ArrayList<>();
        StringBuilder buffer = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // 跳过空行和单行注释
                if (line.isEmpty() || line.startsWith("--"))
                    continue;
                buffer.append(line).append(" ");

                // 分号作为语句结束符
                if (line.endsWith(";")) {
                    String sql = buffer.toString().replaceAll(";\\s*$", "");
                    sqlList.add(sql);
                    buffer.setLength(0);
                }
            }
        }
        return sqlList;
    }
}
