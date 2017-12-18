1. 在测试机上，目前是田江涛的笔记本的虚拟机，
1. 启动方法：
cd /root/redis/redis-2.8.0

src/redis-server ./redis.conf

测试
./redis-cli -h 127.0.0.1 -p 6379 -a "foo'

# 常用命令


1. 字符串类型的用法就是这么简单，因为是二进制安全的，所以你完全可以把一个图片文件的内容作为字符串来存储。

另外，我们还可以通过字符串类型进行数值操作：

```
127.0.0.1:6379> set mynum "2"
OK
127.0.0.1:6379> get mynum
"2"
127.0.0.1:6379> incr mynum
(integer) 3
127.0.0.1:6379> get mynum
"3"
```

看，在遇到数值操作时，redis会将字符串类型转换成数值。
由于INCR等指令本身就具有原子操作的特性，所以我们完全可以利用redis的INCR、INCRBY、DECR、DECRBY等指令来实现原子计数的效果，假如，在某种场景下有3个客户端同时读取了mynum的值（值为2），然后对其同时进行了加1的操作，那么，最后mynum的值一定是5。不少网站都利用redis的这个特性来实现业务上的统计计数需求。
1. redis数据结构 – lists

redis的另一个重要的数据结构叫做lists，翻译成中文叫做“列表”。
首先要明确一点，redis中的lists在底层实现上并不是数组，而是链表，也就是说对于一个具有上百万个元素的lists来说，在头部和尾部插入一个新元素，其时间复杂度是常数级别的，比如用LPUSH在10个元素的lists头部插入新元素，和在上千万元素的lists头部插入新元素的速度应该是相同的。
虽然lists有这样的优势，但同样有其弊端，那就是，链表型lists的元素定位会比较慢，而数组型lists的元素定位就会快得多。
lists的常用操作包括LPUSH、RPUSH、LRANGE等。我们可以用LPUSH在lists的左侧插入一个新元素，用RPUSH在lists的右侧插入一个新元素，用LRANGE命令从lists中指定一个范围来提取元素。我们来看几个例子：
复制代码 代码如下:
//新建一个list叫做mylist，并在列表头部插入元素"1"
127.0.0.1:6379> lpush mylist "1" 
//返回当前mylist中的元素个数
(integer) 1 
//在mylist右侧插入元素"2"
127.0.0.1:6379> rpush mylist "2" 
(integer) 2
//在mylist左侧插入元素"0"
127.0.0.1:6379> lpush mylist "0" 
(integer) 3
//列出mylist中从编号0到编号1的元素
127.0.0.1:6379> lrange mylist 0 1 
1) "0"
2) "1"
//列出mylist中从编号0到倒数第一个元素
127.0.0.1:6379> lrange mylist 0 -1 
1) "0"
2) "1"
3) "2"
lists的应用相当广泛，随便举几个例子：
1.我们可以利用lists来实现一个消息队列，而且可以确保先后顺序，不必像MySQL那样还需要通过ORDER BY来进行排序。
2.利用LRANGE还可以很方便的实现分页的功能。
3.在博客系统中，每片博文的评论也可以存入一个单独的list中。
1. redis数据结构 – 集合

redis的集合，是一种无序的集合，集合中的元素没有先后顺序。
集合相关的操作也很丰富，如添加新元素、删除已有元素、取交集、取并集、取差集等。我们来看例子：
复制代码 代码如下:
//向集合myset中加入一个新元素"one"
127.0.0.1:6379> sadd myset "one" 
(integer) 1
127.0.0.1:6379> sadd myset "two"
(integer) 1
//列出集合myset中的所有元素
127.0.0.1:6379> smembers myset 
1) "one"
2) "two"
//判断元素1是否在集合myset中，返回1表示存在
127.0.0.1:6379> sismember myset "one" 
(integer) 1
//判断元素3是否在集合myset中，返回0表示不存在
127.0.0.1:6379> sismember myset "three" 
(integer) 0
//新建一个新的集合yourset
127.0.0.1:6379> sadd yourset "1" 
(integer) 1
127.0.0.1:6379> sadd yourset "2"
(integer) 1
127.0.0.1:6379> smembers yourset
1) "1"
2) "2"
//对两个集合求并集
127.0.0.1:6379> sunion myset yourset 
1) "1"
2) "one"
3) "2"
4) "two"
对于集合的使用，也有一些常见的方式，比如，QQ有一个社交功能叫做“好友标签”，大家可以给你的好友贴标签，比如“大美女”、“土豪”、“欧巴”等等，这时就可以使用redis的集合来实现，把每一个用户的标签都存储在一个集合之中。
1. redis数据结构 – 有序集合

redis不但提供了无需集合（sets），还很体贴的提供了有序集合（sorted sets）。有序集合中的每个元素都关联一个序号（score），这便是排序的依据。
很多时候，我们都将redis中的有序集合叫做zsets，这是因为在redis中，有序集合相关的操作指令都是以z开头的，比如zrange、zadd、zrevrange、zrangebyscore等等
老规矩，我们来看几个生动的例子：
//新增一个有序集合myzset，并加入一个元素baidu.com，给它赋予的序号是1：
复制代码 代码如下:
127.0.0.1:6379> zadd myzset 1 baidu.com 
(integer) 1
//向myzset中新增一个元素360.com，赋予它的序号是3
127.0.0.1:6379> zadd myzset 3 360.com 
(integer) 1
//向myzset中新增一个元素google.com，赋予它的序号是2
127.0.0.1:6379> zadd myzset 2 google.com 
(integer) 1
//列出myzset的所有元素，同时列出其序号，可以看出myzset已经是有序的了。
127.0.0.1:6379> zrange myzset 0 -1 with scores 
1) "baidu.com"
2) "1"
3) "google.com"
4) "2"
5) "360.com"
6) "3"
//只列出myzset的元素
127.0.0.1:6379> zrange myzset 0 -1 
1) "baidu.com"
2) "google.com"
3) "360.com"
【redis数据结构 – 哈希】
最后要给大家介绍的是hashes，即哈希。哈希是从redis-2.0.0版本之后才有的数据结构。
hashes存的是字符串和字符串值之间的映射，比如一个用户要存储其全名、姓氏、年龄等等，就很适合使用哈希。
我们来看一个例子：
复制代码 代码如下:
//建立哈希，并赋值
127.0.0.1:6379> HMSET user:001 username antirez password P1pp0 age 34 
OK
//列出哈希的内容
127.0.0.1:6379> HGETALL user:001 
1) "username"
2) "antirez"
3) "password"
4) "P1pp0"
5) "age"
6) "34"
//更改哈希中的某一个值
127.0.0.1:6379> HSET user:001 password 12345 
(integer) 0
//再次列出哈希的内容
127.0.0.1:6379> HGETALL user:001 
1) "username"
2) "antirez"
3) "password"
4) "12345"
5) "age"
6) "34"

#详细命令

http://blog.csdn.net/fengyong7723131/article/details/52956130
Redis提供了丰富的命令（command）对数据库和各种数据类型进行操作，这些command可以在Linux终端使用。在编程时，比如使用Redis 的Java语言包，这些命令都有对应的方法。下面将Redis提供的命令做一总结。

官网命令列表：http://redis.io/commands （英文）

1. 连接操作相关的命令

quit：关闭连接（connection）
auth：简单密码认证

1. 对value操作的命令

exists(key)：确认一个key是否存在
del(key)：删除一个key
type(key)：返回值的类型
keys(pattern)：返回满足给定pattern的所有key
randomkey：随机返回key空间的一个key
rename(oldname, newname)：将key由oldname重命名为newname，若newname存在则删除newname表示的key
dbsize：返回当前数据库中key的数目
expire：设定一个key的活动时间（s）
ttl：获得一个key的活动时间
select(index)：按索引查询
move(key, dbindex)：将当前数据库中的key转移到有dbindex索引的数据库
flushdb：删除当前选择数据库中的所有key
flushall：删除所有数据库中的所有key

1. 对String操作的命令

set(key, value)：给数据库中名称为key的string赋予值value
get(key)：返回数据库中名称为key的string的value
getset(key, value)：给名称为key的string赋予上一次的value
mget(key1, key2,…, key N)：返回库中多个string（它们的名称为key1，key2…）的value
setnx(key, value)：如果不存在名称为key的string，则向库中添加string，名称为key，值为value
setex(key, time, value)：向库中添加string（名称为key，值为value）同时，设定过期时间time
mset(key1, value1, key2, value2,…key N, value N)：同时给多个string赋值，名称为key i的string赋值value i
msetnx(key1, value1, key2, value2,…key N, value N)：如果所有名称为key i的string都不存在，则向库中添加string，名称key i赋值为value i
incr(key)：名称为key的string增1操作
incrby(key, integer)：名称为key的string增加integer
decr(key)：名称为key的string减1操作
decrby(key, integer)：名称为key的string减少integer
append(key, value)：名称为key的string的值附加value
substr(key, start, end)：返回名称为key的string的value的子串

1. 对List操作的命令

rpush(key, value)：在名称为key的list尾添加一个值为value的元素
lpush(key, value)：在名称为key的list头添加一个值为value的 元素
llen(key)：返回名称为key的list的长度
lrange(key, start, end)：返回名称为key的list中start至end之间的元素（下标从0开始，下同）
ltrim(key, start, end)：截取名称为key的list，保留start至end之间的元素
lindex(key, index)：返回名称为key的list中index位置的元素
lset(key, index, value)：给名称为key的list中index位置的元素赋值为value
lrem(key, count, value)：删除count个名称为key的list中值为value的元素。count为0，删除所有值为value的元素，count>0从头至尾删除count个值为value的元素，count<0从尾到头删除|count|个值为value的元素。 lpop(key)：返回并删除名称为key的list中的首元素 rpop(key)：返回并删除名称为key的list中的尾元素 blpop(key1, key2,… key N, timeout)：lpop命令的block版本。即当timeout为0时，若遇到名称为key i的list不存在或该list为空，则命令结束。如果timeout>0，则遇到上述情况时，等待timeout秒，如果问题没有解决，则对keyi+1开始的list执行pop操作。
brpop(key1, key2,… key N, timeout)：rpop的block版本。参考上一命令。
rpoplpush(srckey, dstkey)：返回并删除名称为srckey的list的尾元素，并将该元素添加到名称为dstkey的list的头部

1. 对Set操作的命令

sadd(key, member)：向名称为key的set中添加元素member
srem(key, member) ：删除名称为key的set中的元素member
spop(key) ：随机返回并删除名称为key的set中一个元素
smove(srckey, dstkey, member) ：将member元素从名称为srckey的集合移到名称为dstkey的集合
scard(key) ：返回名称为key的set的基数
sismember(key, member) ：测试member是否是名称为key的set的元素
sinter(key1, key2,…key N) ：求交集
sinterstore(dstkey, key1, key2,…key N) ：求交集并将交集保存到dstkey的集合
sunion(key1, key2,…key N) ：求并集
sunionstore(dstkey, key1, key2,…key N) ：求并集并将并集保存到dstkey的集合
sdiff(key1, key2,…key N) ：求差集
sdiffstore(dstkey, key1, key2,…key N) ：求差集并将差集保存到dstkey的集合
smembers(key) ：返回名称为key的set的所有元素
srandmember(key) ：随机返回名称为key的set的一个元素

1. 对zset（sorted set）操作的命令


zadd(key, score, member)：向名称为key的zset中添加元素member，score用于排序。如果该元素已经存在，则根据score更新该元素的顺序。
zrem(key, member) ：删除名称为key的zset中的元素member
zincrby(key, increment, member) ：如果在名称为key的zset中已经存在元素member，则该元素的score增加increment；否则向集合中添加该元素，其score的值为increment
zrank(key, member) ：返回名称为key的zset（元素已按score从小到大排序）中member元素的rank（即index，从0开始），若没有member元素，返回“nil”
zrevrank(key, member) ：返回名称为key的zset（元素已按score从大到小排序）中member元素的rank（即index，从0开始），若没有member元素，返回“nil”
zrange(key, start, end)：返回名称为key的zset（元素已按score从小到大排序）中的index从start到end的所有元素
zrevrange(key, start, end)：返回名称为key的zset（元素已按score从大到小排序）中的index从start到end的所有元素
zrangebyscore(key, min, max)：返回名称为key的zset中score >= min且score <= max的所有元素 zcard(key)：返回名称为key的zset的基数 zscore(key, element)：返回名称为key的zset中元素element的score zremrangebyrank(key, min, max)：删除名称为key的zset中rank >= min且rank <= max的所有元素 zremrangebyscore(key, min, max) ：删除名称为key的zset中score >= min且score <= max的所有元素
zunionstore / zinterstore(dstkeyN, key1,…,keyN, WEIGHTS w1,…wN, AGGREGATE SUM|MIN|MAX)：对N个zset求并集和交集，并将最后的集合保存在dstkeyN中。对于集合中每一个元素的score，在进行AGGREGATE运算前，都要乘以对于的WEIGHT参数。如果没有提供WEIGHT，默认为1。默认的AGGREGATE是SUM，即结果集合中元素的score是所有集合对应元素进行SUM运算的值，而MIN和MAX是指，结果集合中元素的score是所有集合对应元素中最小值和最大值。

1. 对Hash操作的命令

hset(key, field, value)：向名称为key的hash中添加元素field<—>value
hget(key, field)：返回名称为key的hash中field对应的value
hmget(key, field1, …,field N)：返回名称为key的hash中field i对应的value
hmset(key, field1, value1,…,field N, value N)：向名称为key的hash中添加元素field i<—>value i
hincrby(key, field, integer)：将名称为key的hash中field的value增加integer
hexists(key, field)：名称为key的hash中是否存在键为field的域
hdel(key, field)：删除名称为key的hash中键为field的域
hlen(key)：返回名称为key的hash中元素个数
hkeys(key)：返回名称为key的hash中所有键
hvals(key)：返回名称为key的hash中所有键对应的value
hgehtall(key)：返回名称为key的hash中所有的键（field）及其对应的value
8、持久化

save：将数据同步保存到磁盘
bgsave：将数据异步保存到磁盘
lastsave：返回上次成功将数据保存到磁盘的Unix时戳
shundown：将数据同步保存到磁盘，然后关闭服务

1. 远程服务控制

info：提供服务器的信息和统计
monitor：实时转储收到的请求
slaveof：改变复制策略设置
config：在运行时配置Redis服务器
Redis高级应用

1. 安全性
设置客户端连接后进行任何操作指定前需要密码，一个外部用户可以再一秒钟进行150W次访问，具体操作密码修改设置redis.conf里面的requirepass属性给予密码，当然我这里给的是primos
之后如果想操作可以采用登陆的时候就授权使用:
sudo /opt/java/redis/bin/redis-cli -a primos
或者是进入以后auth primos然后就可以随意操作了

1. 主从复制

做这个操作的时候我准备了两个虚拟机，ip分别是192.168.15.128和192.168.15.133
通过主从复制可以允许多个slave server拥有和master server相同的数据库副本
具体配置是在slave上面配置slave
slaveof 192.168.15.128 6379
masterauth primos
如果没有主从同步那么就检查一下是不是防火墙的问题，我用的是ufw，设置一下sudo ufw allow 6379就可以了
这个时候可以通过info查看具体的情况

1. 事务处理

redis对事务的支持还比较简单，redis只能保证一个client发起的事务中的命令可以连续执行，而中间不会插入其他client的命令。当一个client在一个连接中发出multi命令时，这个连接会进入一个事务的上下文，连接后续命令不会立即执行，而是先放到一个队列中，当执行exec命令时，redis会顺序的执行队列中的所有命令。
比如我下面的一个例子
set age 100
multi
set age 10
set age 20
exec
get age –这个内容就应该是20
multi
set age 20
set age 10
exec
get age –这个时候的内容就成了10，充分体现了一下按照队列顺序执行的方式
discard 取消所有事务，也就是事务回滚
不过在redis事务执行有个别错误的时候，事务不会回滚，会把不错误的内容执行，错误的内容直接放弃，目前最新的是2.6.7也有这个问题的
乐观锁
watch key如果没watch的key有改动那么outdate的事务是不能执行的

1. 持久化机制

redis是一个支持持久化的内存数据库
snapshotting快照方式，默认的存储方式，默认写入dump.rdb的二进制文件中，可以配置redis在n秒内如果超过m个key被修改过就自动做快照
append-only file aof方式，使用aof时候redis会将每一次的函 数都追加到文件中，当redis重启时会重新执行文件中的保存的写命
令在内存中。
1. 发布订阅消息 sbusribe publish操作，其实就类似linux下面的消息发布

1. 虚拟内存的使用

可以配置vm功能，保存路径，最大内存上线，页面多少，页面大小，最大工作线程
临时修改ip地址ifconfig eth0 192.168.15.129

redis-cli命令抄袭点击打开链接

redis-cli参数
Usage: redis-cli [OPTIONS] [cmd [arg [arg …]]]
-h Server hostname (default: 127.0.0.1)
-p Server port (default: 6379)
-s Server socket (overrides hostname and port)
-a Password to use when connecting to the server
-r 选项重复执行一个命令指定的次数。
-i 设置命令执行的间隔。
It is possible to specify sub-second times like -i 0.1
-n Database number
-x 选项从标准输入（stdin）读取最后一个参数
-d Multi-bulk delimiter in for raw formatting (default: \n)
-c 开启reidis cluster模式，连接redis cluster节点时候使用
–raw Use raw formatting for replies (default when STDOUT is not a tty)
–latency Enter a special mode continuously sampling latency
–slave 模拟slave从master上接收到的commands。slave上接收到的commands都是update操作，记录数据的更新行为。
–pipe 这个一个非常有用的参数。发送原始的redis protocl格式数据到服务器端执行。
–bigkeys Sample Redis keys looking for big keys
–eval Send an EVAL command using the Lua script at
–help Output this help and exit
–version Output version and exit

Examples:
cat /etc/passwd | redis-cli -x set mypasswd
redis-cli get mypasswd
redis-cli -r 100 lpush mylist x
redis-cli -r 100 -i 1 info | grep used_memory_human:
redis-cli –eval myscript.lua key1 key2 , arg1 arg2 arg3
(Note: when using –eval the comma separates KEYS[] from ARGV[] items)

常用命令：
1） 查看keys个数
keys * // 查看所有keys
keys prefix_* // 查看前缀为”prefix_”的所有keys

2） 清空数据库
flushdb // 清除当前数据库的所有keys
flushall // 清除所有数据库的所有keys
