# 目的
测试多进程并发从一个配置表中取数，要求相互不影响，不重复。
# 测试过程

1. 创建表

  ```
create table alf_test_2
(id serial NOT NULL
,name character(32)
,cnt  integer
,flag character(5)
,date_time  character(20)
,ospid       character(32)
);
  ```

1. 生产随机字符串，长度任意

  ```
drop  function charstring(a1 integer);
create or replace function charstring(a1 integer) returns   
text AS $$  
declare i integer; 
  text_string varchar(128);
begin 
  text_string='';
  FOR i IN 1..a1 LOOP
  text_string =text_string||chr((random()*(122-97)+97)::int);
  end loop;  
  RETURN text_string;
end;  
$$ LANGUAGE plpgsql;  
   ```
1. 生成数据
insert into alf_test2(id,name,cnt,flag) 
select generate_series(1,50000),charstring(5),'0','0' ;



1. new.sql
with t as (select id from alf_test2 where flag='0' limit 5  for update skip locked) update alf_test2 set flag='1',date_time=now()::char(19),cnt=cnt+1,ospid=floor(random()*25)::int where alf_test2.id in (select t.id from t);

1. 批量测试

pgbench -M prepared -n -r -P 1 -f ./new.sql -c 64 -j 64 -T 10

1. 检查

select ospid,count(ospid) from alf_test2 group by ospid