package advanced.chapter6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.umd.cloud9.collection.XMLInputFormat;

public class LSA {

	public static void main(String[] args) {
		//初始化SparkConf
		SparkConf sc = new SparkConf().setMaster("local").setAppName("AnomalyDetectionInNetworkTraffic");
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		JavaSparkContext jsc = new JavaSparkContext(sc);

		//读入数据,在执行前需要先执行WriteToHadoop
		String path = "/user/ds/wikidump.xml";
		Configuration conf = new Configuration();
		conf.set(XMLInputFormat.START_TAG_KEY, "<page>");
		conf.set(XMLInputFormat.END_TAG_KEY, "</page>");
		JavaPairRDD<LongWritable, Text> kvs = jsc.newAPIHadoopFile(path, XMLInputFormat.class, LongWritable.class, Text.class, conf);
		JavaRDD<String> rawXmls = kvs.map(p -> p.toString());
		
		rawXmls.foreach(x -> System.out.println(x));
		
		jsc.close();
	}
}
	
