## 客户端读取HDFS中的数据

- step 1:
> 客户端通过调用FileSystem对象的open()方法来打开希望读取的文件，对于HDFS来说，这个对象是分布式文件系统的一个实例。<br/>

- step 2:
> DistributedFileSystem通过使用RPC来调用namenode，以确定文件起始块的位置<br/>
> 对于每一个块，namenode返回存有该块副本的datanode地址。<br/>
> 如果该客户端本身就是一个datanode（比如，在一个MapReduce任务中），并保存有相应数据块的一个复本时，该节点将从本地datanode中读取数据。<br/>

> DistributedFileSystem类返回一个FSDataInputStream对象（一个支持文件定位的输入流）给客户端并读取数据。<br/>
> FSDataInputStream类转而封装DFSInputStream对象，该对象管理者datanode和namenode的I/O。<br/>

- step 3
> 客户端对这个输入流调用read()方法。<br/>
> 存储着文件起始块的datanode地址的DFSInputStream随即连接距离最近的datanode。<br/>

- step 4
> 通过对数据流反复调用read()方法，可以将数据从datanode传输到客户端。<br/>

- step 5
> 到达块的末端时，DFSInputStream会关闭与该datanode的连接，然后寻找下一个块的最佳datanode。<br/>
> 户端只需要读取连续的流，并且对于客户端都是透明的。<br/>

- step 6
> 客户端从流中读取数据时，块是按照打开DFSInputStream与datanode新建连接的顺序读取的。<br/>
> 它也需要询问namenode来检索下一批所需块的datanode的位置。<br/>
> 一旦客户端完成读取，就对FSDataInputStream调用close()方法。

#### Key Point
```bash
在读取数据的时候，如果DFSInputStream在与datanode通信时遇到错误，它便会尝试从这个块的另外一个最邻近datanode读取数据。
它也会记住那个故障datanode，以保证以后不会反复读取该节点上后续的块。
DFSInputStream也会通过校验和确认从datanode发来的数据是否完整。
如果发现一个损坏的块，它就会在DFSInputStream试图从其他datanode读取一个块的复本之前通知namenode。

这个设计的一个重点是，namenode告知客户端每个块中最佳的datanode，并让客户端直接联系该datanode且检索数据。
由于数据流分散在该集群中的所有datanode，所以这种设计能使HDFS可扩展到大量的并发客户端。同时，namenode仅需要响应块位置的请求
（这些信息存储在内存中，因为非常搞笑），而无需响应数据请求，否则随着客户端数量的增长，namenode很快会成为一个瓶颈。
```