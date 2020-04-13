## groupByKey、reduceByKey、aggregateByKey、distinct原理差别

** groupByKey()是对RDD中的所有数据做shuffle,根据不同的Key映射到不同的partition中再进行aggregate

**aggregateByKey()是先对每个partition中的数据根据不同的Key进行aggregate，然后将结果进行shuffle，完成各个partition之间的aggregate。因此，和groupByKey()相比，运算量小了很多

**reduceByKey()也是先在单台机器中计算，再将结果进行shuffle，减小运算量，减少网络IO，但需要考虑单台机器的内存限制，当shuffle的数据量大于内存总量时 Spark 会把数据保存到磁盘上。 不过在保存时每次会处理一个 key 的数据，所以当单个 key 的键值对超过内存容量会存在内存溢出的异常

**distinct()也是对RDD中的所有数据做shuffle进行aggregate后再去重
