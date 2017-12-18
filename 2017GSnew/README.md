# 关于获取表大小和行数的前后工作
1. 开始是陈使用shell来处理的，手工把表名导出，拼成insert，插入进去，但是应该是一个连接在执行，时间大约需要48个小时。
1. 测试了一下，发现把导出的insert分成14个文件，每个文件10000+的话，大约两个小时就能执行完getsize
1. 测试了一下python程序，发现使用多thread可以达到上述的目的。
1. 使用crontab调用python的gettablesize4进行测试，发现了一些小问题，解决了。
1. 重新检查整体处理过程，怀疑每天处理的数据中有大量重复数据，经过连续几天的测试，结果发现，全量表140000+，其中session表就有40000+，然后，非分区表每天大小可能会变，需要重新统计，分区只需要统计新增加的部分就行了，这样下来，每天只需要处理20000+，第一次需要处理100000+左右就行了。重新设计表和取数sql，session表还是统计吧，但是不取走到配置库

取数据sql：
select schemaname,tablename from pg_tables where tablename not like '%_1_prt_%' and schemaname<>'session' 
and schemaname<>'public'
and schemaname<>'information_schema'
and schemaname<>'pg_catalog'
and schemaname<>'gp_toolkit'
and schemaname||'.'||tablename not in (select distinct schemaname||'.'||tablename from pg_partitions)--去除分区父表以及其他模式的非分区表
union all
select partitionschemaname schemaname,partitiontablename relname from  pg_partitions 
except
select schemaname,relname from dis.tc_table_space_tj_d where relname like '%_1_prt_%'
后续修改过，以代码为准


每天的insert的sql：
insert into dis.tc_table_space_tj_d(schemaname,relname,size,rownum,deal_date)
select 'mk' schemaname,'tm_bq_g3_user_info_3d_1_prt_d20170926' tablename, pg_relation_size('mk.tm_bq_g3_user_info_3d_1_prt_d20170926')
,(select count(1)  from mk.tm_bq_g3_user_info_3d_1_prt_d20170926),cast(to_char(current_timestamp,'yyyymmdd') as int)