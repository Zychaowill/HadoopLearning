#### Note

> 同一个consumer group(group.id相等)下只能有一个消费者可以消费，这个刚开始确实会让很多人踩坑

##### 多线程消费
我们可以利用``` partition```的分区特性来提高消费能力，单线程的时候等于是一个线程要把所有分区里的数据都消费一遍，
如果换成多线程就可以让一个线程只消费一个分区，这样效率自然就提高了，所以线程数``` coresize<=partition ```

#### Reference link
[Kafka](https://crossoverjie.top/2017/10/20/SSM17/?hmsr=toutiao.io&utm_medium=toutiao.io&utm_source=toutiao.io)