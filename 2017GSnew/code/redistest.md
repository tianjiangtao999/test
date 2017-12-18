# 记录了redis实战的源代码测试过程和理解
## ch01
1. 说明：如果想要直接使用文章中的代码或者是调整，需要：

```
import sys
sys.path.insert(0,'/root/gs/2017GS/document/Carlson_Redis_sourcecode/ch01/listings')
from ch01_listing_source import *
```

然后使用下面的命令获得连接进行调试：

```
import redis
conn = redis.Redis(host='192.168.6.128',port='6379',db=15)
```
下面是发布新的文章,article id如果成功会返回文章id

```
article_id = str(post_article(conn, 'test1', 'A title', 'http://www.google.com'))
```
如果不引入pprint，那么r输出的就是一个对象，

'''
import pprint
print r
r = conn.hgetall('article:' + '1')
r = conn.hgetall('article:1' )


article_vote(conn, 'other_user', 'article:1')
r = conn.hgetall('article:2' )
print r
article_vote(conn, 'tainxinyue', 'article:1')
r = conn.hgetall('article:2' )
print r
history