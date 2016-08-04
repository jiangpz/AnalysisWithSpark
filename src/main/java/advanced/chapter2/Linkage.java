package advanced.chapter2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

public class Linkage {
	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setMaster("local").setAppName("Linkage");
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		JavaRDD<String> lines = jsc.textFile("src/main/java/advanced/chapter2/linkage/block_*.csv");
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
		
		JavaRDD<MatchData> parsed = noheader.map(line -> parseLine(line));
		parsed.cache();
		//输出第一个MatchData
		//System.out.println(parsed.first().toString());
		
		Map<Boolean, Long> matchCounts = parsed.map(md -> md.getMatched()).countByValue();
		System.out.println(matchCounts.size());
		
		//查看scores的统计值
		for (int i = 0; i < 9; i++) {
			final Integer innerI = new Integer(i);
			StatCounter statCounter = parsed.mapToDouble(md -> md.getScores()[innerI]).filter(score -> !Double.isNaN(score)).stats();
			System.out.println(statCounter);
		}
		
		JavaRDD<Object> nasRDD = parsed.map(md -> Arrays.asList(md.getScores()).stream().map(d -> new NAStatCounter()));
		jsc.close();
	}

	/**
	 * 
	 * @Title: parseLine
	 * @Description: 将采集的数据转换成对象
	 * @param: @param 采集的单条数据
	 * @return: MatchData
	 * @throws:
	 */
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
		Boolean matched = Boolean.parseBoolean(pieces[1]);
		MatchData matchData = new MatchData();
		matchData.setId1(id1);
		matchData.setId2(id2);
		matchData.setScores(scores);
		matchData.setMatched(matched);
		return matchData;
	}
}
