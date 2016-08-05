package advanced.chapter2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;

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
		
		//查看scores的统计值，2.10中对应的代码
		for (int i = 0; i < 9; i++) {
			final Integer innerI = new Integer(i);
			StatCounter statCounter = parsed.mapToDouble(md -> md.getScores()[innerI]).filter(score -> !Double.isNaN(score)).stats();
			System.out.println(statCounter);
		}
		
		ststsWithMissing(parsed);
		
		List<NAStatCounter> ststsm = ststsWithMissing(parsed.filter(md -> md.getMatched()));
		List<NAStatCounter> ststsn = ststsWithMissing(parsed.filter(md -> !md.getMatched()));
		
		List<Tuple2<NAStatCounter, NAStatCounter>> ststs = new ArrayList<Tuple2<NAStatCounter, NAStatCounter>>();
		for (int i = 0; i < ststsm.size(); i++) {
			ststs.add(new Tuple2<NAStatCounter, NAStatCounter>(ststsm.get(i), ststsn.get(i)));
		}
		System.out.println("评分：");
		ststs.stream().forEach(p -> System.out.println((p._1.missing+p._2.missing)+","+(p._1.stats.mean()-p._2.stats.mean())));
		
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
		Boolean matched = Boolean.parseBoolean(pieces[11]);
		MatchData matchData = new MatchData();
		matchData.setId1(id1);
		matchData.setId2(id2);
		matchData.setScores(scores);
		matchData.setMatched(matched);
		return matchData;
	}
	
	/**
	 * 
	 * @Title: ststsWithMissing
	 * @Description: 2.11 中对应的代码
	 * @param: @param parsed    参数
	 * @return 
	 * @return: void    返回类型
	 * @throws:
	 */
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
}
