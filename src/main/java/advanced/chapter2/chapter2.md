数据清洗时数据科学项目的第一步，往往也是最重要的一步。

Spark编程模型
编写Spark程序通常包括一系列相关步骤：
	1. 在输入数据集上定义一组转换。
	2. 调用action，用以将转换后的数据集保存到持久存储上，或者把结果返回到驱动程序的本地内存。
	3. 运行本地计算，本地计算处理分布式计算的结果。本地计算有助于你确定下一步的转换和action。

2.4 小试牛刀：Spark shell和SparkContext
加州大学欧文分校机器学习资料库(UC Irvine Machine Learning Repository)，这个资料库为研究和教学提供了大量非常好的数据源，这些数据源非常有意义，并且是免费的。

首先，我们从资料库中下载数据（需翻墙，也可以从此处下载(http://pan.baidu.com/s/1c29fBVy)）：
$ mkdir linkage
$ cd linkage/
$ curl -o donation.zip http://bit.ly/1Aoywaq
$ unzip donation.zip
$ unzip 'block_*.zip'

我在学习时没有使用Hadoop集群，数据就放到了源码中，主要解压后的数据放到了一个文件夹下。同时，这本书使用的语言是Scala，在学习时，会再写一遍对应的Java版本。

创建RDD：

Scala：
scala> val rawblocks = sc.textFile("D:/Workspace/AnalysisWithSpark/src/main/java/advanced/chapter2/linkage")
rawblocks: org.apache.spark.rdd.RDD[String] = D:/Workspace/AnalysisWithSpark/src/main/java/advanced/chapter2/linkage MapPartitionsRDD[3] at textFile at <console>:27

Java：
SparkConf sc = new SparkConf().setMaster("local").setAppName("Linkage");
System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
JavaSparkContext jsc = new JavaSparkContext(sc);
JavaRDD<String> lines = jsc.textFile("src/main/java/advanced/chapter2/linkage");

2.5 把数据从集群上获取到客户端
使用first向客户端返回RDD的第一个元素：

Scala：
scala> rawblocks.first
res2: String = "id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match"

Java：
System.out.println(lines.first());
如果RDD只包含少量记录，可以使用collect方法向客户返回一个包含所有RDD内容的数组。
take方法可以向客户端返回一个包含指定数量记录的数组。例如：

Scala：
scala> val head = rawblocks.take(10)
head: Array[String] = Array("id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match", 37291,53113,0.833333333333333,?,1,?,1,1,1,1,0,TRUE, 39086,47614,1,?,1,?,1,1,1,1,1,TRUE, 70031,70237,1,?,1,?,1,1,1,1,1,TRUE, 84795,97439,1,?,1,?,1,1,1,1,1,TRUE, 36950,42116,1,?,1,1,1,1,1,1,1,TRUE, 42413,48491,1,?,1,?,1,1,1,1,1,TRUE, 25965,64753,1,?,1,?,1,1,1,1,1,TRUE, 49451,90407,1,?,1,?,1,1,1,1,0,TRUE, 39932,40902,1,?,1,?,1,1,1,1,1,TRUE)

scala> head.length
res4: Int = 10

Java：
//RDD前10个元素
List<String> head = lines.take(10);
System.out.println(head);
System.out.println(head.size());

可以使用foreach方法并结合println来打印出数组中的每个值，并且每一行打印一个值：

Scala：
scala> head.foreach(println)
"id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match"
37291,53113,0.833333333333333,?,1,?,1,1,1,1,0,TRUE
39086,47614,1,?,1,?,1,1,1,1,1,TRUE
70031,70237,1,?,1,?,1,1,1,1,1,TRUE
84795,97439,1,?,1,?,1,1,1,1,1,TRUE
36950,42116,1,?,1,1,1,1,1,1,1,TRUE
42413,48491,1,?,1,?,1,1,1,1,1,TRUE
25965,64753,1,?,1,?,1,1,1,1,1,TRUE
49451,90407,1,?,1,?,1,1,1,1,0,TRUE
39932,40902,1,?,1,?,1,1,1,1,1,TRUE

Java：
//分行打印前10个元素
for (String string : head) {
	System.out.println(string);
}

由于CSV文件有一个标题行需要过滤，以免影响后续分析，书上对此部分有详细介绍，我在这里使用下面的方法：

Scala：
scala> head.filter(x => !x.contains("id_1")).length
res6: Int = 9
或者
Scala：
scala> head.filterNot(_.contains("id_1")).length
res9: Int = 9
或者
Scala：
scala> head.filter(!_.contains("id_1")).length
res8: Int = 9

Java：
//输出不包含id_1的前10行
List<String> headWithout = lines.filter(line -> !line.contains("id_1")).take(10);
for (String string : headWithout) {
	System.out.println(string);
}

其中Java代码其实是先过滤再取的前10行，其实是在集群进行的处理，如果在客户端进行处理只需要对返回List进行过滤，这个就不写了。

Java中似乎没有filterNot。

2.6 把代码从客户端发送到集群

Scala：
scala> val noheader = rawblocks.filter(!_.contains("id_1"))
noheader: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[5] at filter at <console>:29

scala> noheader.first
res11: String = 37291,53113,0.833333333333333,?,1,?,1,1,1,1,0,TRUE

Java：
JavaRDD<String> noheader = lines.filter(line -> !line.contains("id_1"));
System.out.println(noheader.first());

2.7 用元组和case class对数据进行结构化

使用了Scala的隐式类型转换。

Scala：
scala> val line = head(5)
line: String = 36950,42116,1,?,1,1,1,1,1,1,1,TRUE

scala> val pieces = line.split(',')
pieces: Array[String] = Array(36950, 42116, 1, ?, 1, 1, 1, 1, 1, 1, 1, TRUE)

scala> val id1 = pieces(0).toInt
id1: Int = 36950

scala> val id2 = pieces(1).toInt
id2: Int = 42116

scala> val matched = pieces(11).toBoolean
matched: Boolean = true

scala> val rawscores = pieces.slice(2, 11)
rawscores: Array[String] = Array(1, ?, 1, 1, 1, 1, 1, 1, 1)

scala> rawscores.map(s => s.toDouble)
java.lang.NumberFormatException: For input string: "?"
        at sun.misc.FloatingDecimal.readJavaFormatString(Unknown Source)
        at sun.misc.FloatingDecimal.parseDouble(Unknown Source)

遇到错误，此时需要将rawscores 中的"?"在转换成double类型时转换为Double.NaN:

Scala：
scala> def toDouble(s: String) = {
      if ("?".equals(s)) Double.NaN else s.toDouble
      }
toDouble: (s: String)Double

scala> val scores = rawscores.map(toDouble)
scores: Array[Double] = Array(1.0, NaN, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)

Java:
String line = head.get(5);
String pieces[] = line.split(",");
Integer id1 = Integer.parseInt(pieces[0]);
Double scores[] = new Double[pieces.length-3];
Integer id2 = Integer.parseInt(pieces[1]);
for (int i = 2,j = 0; i < pieces.length-1; i++, j++) {
	String piece = pieces[i];
	if(piece.equals("?")){
		scores[j] = Double.NaN;
	} else {
		scores[j] = Double.parseDouble(pieces[i]);
	}
}
Boolean matched = Boolean.parseBoolean(pieces[1]);
MatchData matchData = new MatchData();
matchData.setId1(id1);
matchData.setId2(id2);
matchData.setMatched(matched);
matchData.setMatched(matched);
matchData.toString();

将上面的代码合并成一个函数：
Scala：
scala> val scores = rawscores.map(toDouble)
scores: Array[Double] = Array(1.0, NaN, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)

scala> def parse(line: String) = {
      val pieces = line.split(',')
      val id1 = pieces(0).toInt
      val id2 = pieces(1).toInt
      val scores = pieces.slice(2, 11).map(toDouble)
      val matched = pieces(11).toBoolean
      (id1, id2, scores, matched)
      }
parse: (line: String)(Int, Int, Array[Double], Boolean)

scala> val tup = parse(line)
tup: (Int, Int, Array[Double], Boolean) = (36950,42116,Array(1.0, NaN, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),true)

获取元组单个字段的值可以通过下标函数，从_1开始，或者用productElement方法，它是从0开始计数的。可用productArity方法获得元组大小。

通过定义一个case class来通过有意义的名称来访问记录元素：
Scala：
scala> case class MatchData(id1: Int, id2: Int,
      scores: Array[Double], matched: Boolean)
defined class MatchData

scala> def parse(line: String) = {
      val pieces = line.split(',')
      val id1 = pieces(0).toInt
      val id2 = pieces(1).toInt
      val scores = pieces.slice(2, 11).map(toDouble)
      val matched = pieces(11).toBoolean
      MatchData(id1, id2, scores, matched)
      }
parse: (line: String)MatchData

scala> val md = parse(line)
md: MatchData = MatchData(36950,42116,[D@3be5eccd,true)

scala> md.matched
res5: Boolean = true

scala> md.id1
res6: Int = 36950

Java:
private static MatchData parseLine(String line) {
	String pieces[] = line.split(",");
	Integer id1 = Integer.parseInt(pieces[0]);
	Double[] scores = new Double[pieces.length-3];
	Integer id2 = Integer.parseInt(pieces[1]);
	for (int i = 2,j = 0; i < pieces.length-1; i++, j++) {
		String piece = pieces[i];
		if(piece.equals("?")){
			scores[j] = Double.NaN;
		} else {
			scores[j] = Double.parseDouble(pieces[i]);
		}
	}
	Boolean matched = Boolean.parseBoolean(pieces[11]);
	MatchData matchData = new MatchData();
	matchData.setId1(id1);
	matchData.setId2(id2);
	matchData.setScores(scores);
	matchData.setMatched(matched);
	matchData.toString();
	return matchData;
}

其中MatchData定义如下：
Java:
import java.io.Serializable;
import java.util.Arrays;

public class MatchData implements Serializable{
	private static final long serialVersionUID = 1L;
	private Integer id1;
	private Integer id2;
	private Double[] scores; 
	private Boolean matched;
	public Integer getId1() {
		return id1;
	}
	public void setId1(Integer id1) {
		this.id1 = id1;
	}
	public Integer getId2() {
		return id2;
	}
	public void setId2(Integer id2) {
		this.id2 = id2;
	}
	public Double[] getScores() {
		return scores;
	}
	public void setScores(Double[] scores) {
		this.scores = scores;
	}
	public Boolean getMatched() {
		return matched;
	}
	public void setMatched(Boolean matched) {
		this.matched = matched;
	}
	
	@Override
	public String toString() {
		return "MatchData [id1=" + id1 + ", id2=" + id2 + ", scores=" + Arrays.toString(scores) + ", matched=" + matched
				+ "]";
	}
}

现在单条记录测试完成了，我们将其用在head数组的除了标题的所有元素上：
Scala：
scala> val mds = head.filter(!_.contains("id_1")).map(x => parse(x))
mds: Array[MatchData] = Array(MatchData(37291,53113,[D@5b555e90,true), MatchData(39086,47614,[D@4305d2ec,true), MatchData(70031,70237,[D@cb06460,true), MatchData(84795,97439,[D@1cfa2146,true), MatchData(36950,42116,[D@6daa5d97,true), MatchData(42413,48491,[D@55607c,true), MatchData(25965,64753,[D@fc98772,true), MatchData(49451,90407,[D@42f477dc,true), MatchData(39932,40902,[D@7d0ddcf0,true))

将解析函数用于集群数据：
Scala：
scala> val parsed = noheader.map(line => parse(line))
parsed: org.apache.spark.rdd.RDD[MatchData] = MapPartitionsRDD[3] at map at <console>:53

Java:
JavaRDD<MatchData> parsed = noheader.map(line -> parseLine(line));

缓存RDD：
Scala：
scala> parsed.cache()
res8: parsed.type = MapPartitionsRDD[3] at map at <console>:53

Java:
parsed.cache();
Spark为持久化RDD定义了几种不同的机制，用不同的StorageLevel值表示。
Rdd.cache()是rdd.persist(StorageLevel.MEMORY)的简写，他将RDD存储为未序列化的Java对象。占用很大的内存。内存空间不够时就不存储，使用时需要重新计算。
MEMORY_SER(Spark 1.6之后为MEMORY_ONLY_SER)存放序列化后的内容。使用得当时，序列化数据占用的空间比未序列化数据少两到五倍。
MEMORY_AND_DISK和MEMORY_AND_DISK_SER类似于MEMORY和MEMORY_SER，如果分区在内存中放不下，Spark会将其溢写到磁盘上。

2.8 聚合
对集群数据进行聚合时，一定要时刻记住我们分析的数据是存放在多台机器上的，并且聚合需要通过连接机器的网络来移动数据。跨网络移动数据需要许多计算资源，包括确定每条记录要传到哪些服务器、数据序列化、数据压缩，通过网络发送数据、解压缩，接着序列化结果，最后在聚合后的数据上执行运算。为了提高速度，我们需要尽可能少的移动数据。在聚合前能过滤掉的数据越多，就能越快得到问题的答案。

2.9 创建直方图
使用RDD的countByValue创建直方图，该动作对计数类运算效率非常高，它向客户端返回Map[T,Long]类型的结果。安照MatchData的matched字段：

Scala：
scala>  val matchCounts = parsed.map(md => md.matched).countByValue()
matchCounts: scala.collection.Map[Boolean,Long] = Map(true -> 20931, false -> 5728201)

scala> val matchCountsSeq = matchCounts.toSeq
matchCountsSeq: Seq[(Boolean, Long)] = ArrayBuffer((true,20931), (false,5728201))

scala> matchCountsSeq.sortBy(_._1).foreach(println)
(false,5728201)
(true,20931)

scala> matchCountsSeq.sortBy(_._2).foreach(println)
(true,20931)
(false,5728201)

Java的排序需要使用TreeMap和Comparator，不详细写了。

2.10 连续变量的概要统计
查看scores的值：
Scala：
scala> parsed.map(md => md.scores(0)).stats()
res2: org.apache.spark.util.StatCounter = (count: 5749132, mean: NaN, stdev: NaN, max: NaN, min: NaN)

scala> import java.lang.Double.isNaN
import java.lang.Double.isNaN

scala> parsed.map(md => md.scores(0)).filter(!isNaN(_)).stats()
res3: org.apache.spark.util.StatCounter = (count: 5748125, mean: 0.712902, stdev: 0.388758, max: 1.000000, min: 0.000000)

scala> val stats = (0 until 9).map(i => {
      parsed.map(md => md.scores(i)).filter(!isNaN(_)).stats()
      })
stats: scala.collection.immutable.IndexedSeq[org.apache.spark.util.StatCounter] = Vector((count: 5748125, mean: 0.712902, stdev: 0.388758, max: 1.000000, min: 0.000000), (count: 103698, mean: 0.900018, stdev: 0.271316, max: 1.000000, min: 0.000000), (count: 5749132, mean: 0.315628, stdev: 0.334234, max: 1.000000, min: 0.000000), (count: 2464, mean: 0.318413, stdev: 0.368492, max: 1.000000, min: 0.000000), (count: 5749132, mean: 0.955001, stdev: 0.207301, max: 1.000000, min: 0.000000), (count: 5748337, mean: 0.224465, stdev: 0.417230, max: 1.000000, min: 0.000000), (count: 5748337, mean: 0.488855, stdev: 0.499876, max: 1.000000, min: 0.000000), (count: 5748337, mean: 0.222749, stdev: 0.416091, max: 1.000000, min: 0.000000), (count: 5736289, mean: 0.005529, stdev: 0.074149, max: 1.000000,...

Java：
for (int i = 0; i < 9; i++) {
	final Integer innerI = new Integer(i);
	StatCounter statCounter = parsed.mapToDouble(md -> md.getScores()[innerI]).filter(score -> !Double.isNaN(score)).stats();
	System.out.println(statCounter);
}

2.11 为计算概要信息创建可重用代码

上面的方法能够完成工作，但是为了得到所有统计信息必须重复处理parsed RDD的所有记录9次，是每列分别统计一次。现在需要将其改进为一次处理所有记录。先将所有行聚合，然后再处理。
Scala的代码参考书上或者书中附带的源码(https://github.com/sryza/aas/tree/master/ch02-intro/src/main/scala/com/cloudera/datascience/intro)：

Scala：
/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.intro

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

case class MatchData(id1: Int, id2: Int,
  scores: Array[Double], matched: Boolean)
case class Scored(md: MatchData, score: Double)

object RunIntro extends Serializable {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Intro"))
   
    val rawblocks = sc.textFile("hdfs:///user/ds/linkage")
    def isHeader(line: String) = line.contains("id_1")
    
    val noheader = rawblocks.filter(x => !isHeader(x))
    def toDouble(s: String) = {
     if ("?".equals(s)) Double.NaN else s.toDouble
    }

    def parse(line: String) = {
      val pieces = line.split(',')
      val id1 = pieces(0).toInt
      val id2 = pieces(1).toInt
      val scores = pieces.slice(2, 11).map(toDouble)
      val matched = pieces(11).toBoolean
      MatchData(id1, id2, scores, matched)
    }

    val parsed = noheader.map(line => parse(line))
    parsed.cache()

    val matchCounts = parsed.map(md => md.matched).countByValue()
    val matchCountsSeq = matchCounts.toSeq
    matchCountsSeq.sortBy(_._2).reverse.foreach(println)

    val stats = (0 until 9).map(i => {
      parsed.map(_.scores(i)).filter(!_.isNaN).stats()
    })
    stats.foreach(println)

    val nasRDD = parsed.map(md => {
      md.scores.map(d => NAStatCounter(d))
    })
    val reduced = nasRDD.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
    reduced.foreach(println)

    val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
    val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))
    statsm.zip(statsn).map { case(m, n) =>
      (m.missing + n.missing, m.stats.mean - n.stats.mean)
    }.foreach(println)

    def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d
    val ct = parsed.map(md => {
      val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
      Scored(md, score)
    })

    ct.filter(s => s.score >= 4.0).
      map(s => s.md.matched).countByValue().foreach(println)
    ct.filter(s => s.score >= 2.0).
      map(s => s.md.matched).countByValue().foreach(println)
  }

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
      val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
      iter.foreach(arr => {
        nas.zip(arr).foreach { case (n, d) => n.add(d) }
      })
      Iterator(nas)
    })
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
  }
}

class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter = {
    if (x.isNaN) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }

  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString: String = {
    "stats: " + stats.toString + " NaN: " + missing
  }
}

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}

Java测试相关代码
Java:
public static void main(String[] args) {
	//测试NAStatCounter类
	NAStatCounter nas1 = new NAStatCounter(10.0);
	nas1.add(2.1);
	NAStatCounter nas2 = new NAStatCounter(Double.NaN);
	nas1.merge(nas2);
	System.out.println("nas1:");
	System.out.println(nas1.toString());
	
	//测试List统计
	NAStatCounter nas3 = new NAStatCounter();
	Arrays.asList(new Double[]{1.0, Double.NaN, 17.29}).forEach(d -> nas3.add(d));
	System.out.println("nas3:");
	System.out.println(nas3);
	NAStatCounter nas4 = new NAStatCounter();
	Arrays.asList(new Double[]{Double.NaN, 15.39, 2.0}).forEach(d -> nas4.add(d));
	System.out.println("nas4:");
	System.out.println(nas4);
	
	//测试聚合
	List<NAStatCounter> list1 = Arrays.asList(new Double[]{1.0, Double.NaN, 17.29})
									 .stream()
									 .map(d -> new NAStatCounter(d))
									 .collect(Collectors.toList());
	System.out.println("list1:");
	for (NAStatCounter naStatCounter : list1) {
		System.out.println(naStatCounter);
	}
	List<NAStatCounter> list2 = Arrays.asList(new Double[]{Double.NaN, 15.39, 2.0})
			.stream()
			.map(d -> new NAStatCounter(d))
			.collect(Collectors.toList());
	System.out.println("list2:");
	for (NAStatCounter naStatCounter : list2) {
		System.out.println(naStatCounter);
	}
	List<Tuple2<NAStatCounter, NAStatCounter>> n0 = new ArrayList<Tuple2<NAStatCounter, NAStatCounter>>();
	for (int i = 0; i < list1.size(); i++) {
		n0.add(new Tuple2<NAStatCounter, NAStatCounter>(list1.get(i), list2.get(i)));
	}
	List<NAStatCounter> merged = n0.stream().map(p -> p._1.merge(p._2)).collect(Collectors.toList());
	System.out.println("merged:");
	merged.forEach(System.out::println);
}

Java最终代码：

Java：
public static List<NAStatCounter> ststsWithMissing(JavaRDD<MatchData> parsed){
	JavaRDD<List<NAStatCounter>> nasRDD = parsed.map(md -> {
		return Arrays.asList(md.getScores()).stream().map(d -> new NAStatCounter(d)).collect(Collectors.toList());
	});
	List<NAStatCounter> reduced = nasRDD.reduce((List<NAStatCounter> n1, List<NAStatCounter>n2) -> {
		List<Tuple2<NAStatCounter, NAStatCounter>> n0 = new ArrayList<Tuple2<NAStatCounter, NAStatCounter>>();
		for (int i = 0; i < n1.size(); i++) {
			n0.add(new Tuple2<NAStatCounter, NAStatCounter>(n1.get(i), n2.get(i)));
		}
		return n0.stream().map(p -> p._1.merge(p._2)).collect(Collectors.toList());
	});
	reduced.forEach(System.out::println);
	return reduced;
}

没有找到Java中可以代替Scala zip函数的好方式，先自己拼了一个，如果好的方式，还请大家不吝赐教。

2.12 变量的选择和评分简介
有了ststsWithMissing函数就可以分享parsed RDD中匹配和不匹配记录的匹配分值数组的分布差异了。

Scala：
val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))
statsm.zip(statsn).map { case(m, n) =>
(m.missing + n.missing, m.stats.mean - n.stats.mean)
}.foreach(println)

Java：
List<NAStatCounter> ststsm = ststsWithMissing(parsed.filter(md -> md.getMatched()));
List<NAStatCounter> ststsn = ststsWithMissing(parsed.filter(md -> !md.getMatched()));

List<Tuple2<NAStatCounter, NAStatCounter>> ststs = new ArrayList<Tuple2<NAStatCounter, NAStatCounter>>();
for (int i = 0; i < ststsm.size(); i++) {
	ststs.add(new Tuple2<NAStatCounter, NAStatCounter>(ststsm.get(i), ststsn.get(i)));
}
System.out.println("评分：");
ststs.stream().forEach(p -> System.out.println((p._1.missing+p._2.missing)+","+(p._1.stats.mean()-p._2.stats.mean())));

结果：
1007,0.285452905746686
5645434,0.09104268062279897
0,0.6838772482597568
5746668,0.8064147192926266
0,0.03240818525033473
795,0.7754423117834044
795,0.5109496938298719
795,0.7762059675300523
12843,0.9563812499852176

评分模型：
Scala：
def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d
case class Scored(md: MatchData, score: Double)
val ct = parsed.map(md => {
val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
Scored(md, score)
})
ct.filter(s => s.score >= 4.0).map(s => s.md.matched).countByValue()
ct.filter(s => s.score >= 2.0).map(s => s.md.matched).countByValue()

Java：
计算ct:
public static JavaRDD<Scored> getCT(JavaRDD<MatchData> parsed){
	JavaRDD<Scored> ct = parsed.map(md -> {
		List<Double> scoreList = Arrays.asList(new Integer[]{2, 5, 6, 7, 8}).stream().map(i -> {
			if(md.getScores()[i].equals(Double.NaN)){
				return 0.0;
			} else {
				return md.getScores()[i];
			}
		}).collect(Collectors.toList());
		Scored scored = new Scored();
		scored.score = scoreList.stream().reduce((r, e) -> r = r + e ).get();
		scored.md = md;
		return scored;
	});
	return ct;
}

调用ct:
JavaRDD<Scored> ct = getCT(parsed);

Map<Boolean, Long> ct0 = ct.filter(s -> s.score >= 4.0).map(s -> s.md.getMatched()).countByValue();
for (Entry<Boolean, Long> ctTmp : ct0.entrySet()) {
	System.out.println("Key:" + ctTmp.getKey());
	System.out.println("Value:" + ctTmp.getValue());
}
Map<Boolean, Long> ct1 = ct.filter(s -> s.score >= 2.0).map(s -> s.md.getMatched()).countByValue();
for (Entry<Boolean, Long> ctTmp : ct1.entrySet()) {
	System.out.println("Key:" + ctTmp.getKey());
	System.out.println("Value:" + ctTmp.getValue());
}

结果：

Key:true
Value:20871
Key:false
Value:637


Key:true
Value:20931
Key:false
Value:596414


