1.1 数据科学面临的挑战
第一，成功的分析中绝大部分工作是数据预处理。
第二，迭代与数据科学紧密相关。建模和分析经常需要对一个数据集进行多次遍历。这其中一方面是由机器学习算法和统计过程本身造成的。
第三，构建完编写卓越的模型不等于大功告成。数据科学的目标在于让数据对不懂科学的人有用。

1.2 认识Apache Spark
Spark继承了MapReduce的线性扩展性和容错性，同事对它做了一些重量级扩展。
Spark摒弃了MapReduce先map再reduce这样的严格方式。
Spark扩展了前辈们的内存计算能力。
在数据处理和ETL方面，Spark的目标是成为大数据界的Python而不是大数据界的Matlab。
Spark还紧密集成Hadoop生态系统里的很多工具。他能镀锡MapReduce支持的所有数据格式，可以与Hadoop上的常用数据格式，如Avro和Parquet（当然也包括古老的CSV），进行交互。它能读写NoSQL数据库，能连续从Flume何Kafka之类的系统读取数据，能和Hive Metastore交互。
Spark相比MapReduce仍然很年轻，其批处理能力仍然比不过MapReduce。

1.3 关于本书
每个实例都自成一体。