package advanced.chapter6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WriteToHadoop {
	public static void main(String[] args) {
		//初始化SparkConf
		SparkConf sc = new SparkConf().setMaster("local").setAppName("AnomalyDetectionInNetworkTraffic");
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		JavaSparkContext jsc = new JavaSparkContext(sc);

		//读入数据
		JavaRDD<String> rawData =jsc.textFile("D:/enwiki-20170220-pages-articles1.xml-p000000010p000030302");
		rawData.saveAsTextFile("/user/ds/wikidump.xml");
		jsc.close();
	}
}
