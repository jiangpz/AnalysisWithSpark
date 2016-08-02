package advanced.chapter2;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Linkage {
	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setMaster("local").setAppName("Linkage");
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		JavaRDD<String> lines = jsc.textFile("src/main/java/advanced/chapter2/linkage");
		//RDD总数
		System.out.println("RDD总数:");
		System.out.println(lines.count());
		//RDD第一个元素
		System.out.println("RDD第一个元素:");
		System.out.println(lines.first());
		//RDD所有元素，需要配置较大的JVM内存，否则会报错
		//System.out.println(lines.collect());
		//RDD前10个元素
		List<String> head = lines.take(10);
		System.out.println(head);
		System.out.println(head.size());
		System.out.println("输出RDD前10个元素:");
		for (String string : head) {
			System.out.println(string);
		}
		//输出不包含id_1的前10行
		System.out.println("输出不包含id_1的前10行:");
		List<String> headWithout = lines.filter(line -> !line.contains("id_1")).take(10);
		for (String string : headWithout) {
			System.out.println(string);
		}
		//获取没有Header的元素
		System.out.println("获取没有Header的元素:");
		JavaRDD<String> noheader = lines.filter(line -> !line.contains("id_1"));
		System.out.println(noheader.first());
		
		jsc.close();
	}

}
