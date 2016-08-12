package advanced.chapter3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public class RecommendingMusic {

	public static void main(String[] args) {
		//初始化SparkConf
		SparkConf sc = new SparkConf().setMaster("local").setAppName("RecommendingMusic");
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		JavaSparkContext jsc = new JavaSparkContext(sc);

		//读入用户-艺术家播放数据
		JavaRDD<String> rawUserArtistData =jsc.textFile("src/main/java/advanced/chapter3/profiledata_06-May-2005/user_artist_data.txt");
		
		//显示数据统计信息
		System.out.println(rawUserArtistData.mapToDouble(line -> Double.parseDouble(line.split(" ")[0])).stats());
		System.out.println(rawUserArtistData.mapToDouble(line -> Double.parseDouble(line.split(" ")[1])).stats());
		
		//读入艺术家ID-艺术家名数据
		JavaPairRDD<Integer, String> artistByID = artistByID(jsc);
		
		//将拼写错误的艺术家ID或非标准的艺术家ID映射为艺术家的正规名
		Map<Integer, Integer> artistAlias  = rawArtistAlias(jsc);
		
		//获取两组艺术家别名
		artistByID.lookup(6803336).forEach(System.out::println);
		artistByID.lookup(1000010).forEach(System.out::println);
		artistByID.lookup(1092764).forEach(System.out::println);
		artistByID.lookup(1000311).forEach(System.out::println);
		
		//数据集转换
		Broadcast<Map<Integer, Integer>> bArtistAlias = jsc.broadcast(artistAlias);
		System.out.println("数据集转换");
		JavaRDD<Rating> trainData = rawUserArtistData.map( line -> {
			List<Integer> list = Arrays.asList(line.split(" ")).stream().map(x -> Integer.parseInt(x)).collect(Collectors.toList());
			bArtistAlias.getValue().getOrDefault(list.get(1), list.get(1));
			return new Rating(list.get(0), list.get(1), list.get(2));
		}).cache();
		
		//构建模型
		System.out.println("构建模型");
		MatrixFactorizationModel model = org.apache.spark.mllib.recommendation.ALS.train(JavaRDD.toRDD(trainData), 10, 5 ,0.01, 1);
		
		System.out.println("输出模型");
		model.userFeatures().toJavaRDD().map(f -> f.toString()).first();
		System.out.println("1");
		model.userFeatures().toJavaRDD().foreach(f -> System.out.println(f._1.toString() + f._2[0] + f._2.toString()));
		
		//逐个检查推荐结果
		JavaRDD<String[]> rawArtistsForUser = rawUserArtistData.map(x -> x.split(" ")).filter(f -> Integer.parseInt(f[0]) == 1000029 );
		List<Integer> existingProducts = rawArtistsForUser.map(f -> Integer.parseInt(f[1])).collect();
		artistByID.filter(f -> existingProducts.contains(f._1)).values().collect().forEach(System.out::println);
		
		//对id为1000029的用户做5个推荐
		Rating[] recommendations = model.recommendProducts(1000029, 5);
		Arrays.asList(recommendations).stream().forEach(System.out::println);
		
		 //查找推荐艺术家ID对应的名字
		List<Integer> recommendedProductIDs = Arrays.asList(recommendations).stream().map(y -> y.product()).collect(Collectors.toList());
		artistByID.filter(f -> recommendedProductIDs.contains(f._1)).values().collect().forEach(System.out::println);
		
		//关闭JavaSparkContext
		jsc.close();
	}
	
	/**
	 * 
	 * @Title: artistByID
	 * @Description: 读入艺术家ID-艺术家名数据
	 * @param: @param jsc
	 * @return: JavaPairRDD<Integer,String>    返回类型
	 */
	public static JavaPairRDD<Integer, String> artistByID(JavaSparkContext jsc) {
		JavaRDD<String> rawArtistData =jsc.textFile("src/main/java/advanced/chapter3/profiledata_06-May-2005/artist_data.txt");
		JavaPairRDD<Integer, String> artistByID = rawArtistData.flatMapToPair(line -> {
			List<Tuple2<Integer, String>> results = new ArrayList<>();
			String[] lineSplit = line.split("\\t", 2);
			if (lineSplit.length == 2) {
				Integer id;
				try {
					id = Integer.parseInt(lineSplit[0]);
				} catch (NumberFormatException e) {
					id = null;
				}
				if(!lineSplit[1].isEmpty() && id != null){
					results.add(new Tuple2<Integer, String>(id, lineSplit[1]));
				}
			}
			return results;
		});
		return artistByID;
	}

	/**
	 * 
	 * @Title: rawArtistAlias
	 * @Description: 将拼写错误的艺术家ID或非标准的艺术家ID映射为艺术家的正规名
	 * @param: @param jsc
	 * @return: Map<Integer,Integer>    返回类型
	 */
	public static Map<Integer, Integer> rawArtistAlias(JavaSparkContext jsc) {
		JavaRDD<String> rawArtistAlias =jsc.textFile("src/main/java/advanced/chapter3/profiledata_06-May-2005/artist_alias.txt");
		Map<Integer, Integer> artistAlias  = rawArtistAlias.flatMapToPair(line -> {
			List<Tuple2<Integer, Integer>> results = new ArrayList<>();
			String[] lineSplit = line.split("\\t", 2);
			if((lineSplit.length == 2 && !lineSplit[0].isEmpty())){
				results.add(new Tuple2<Integer, Integer>(Integer.parseInt(lineSplit[0]), Integer.parseInt(lineSplit[1])));
			}
			return results;
		}).collectAsMap();
		return artistAlias;
	}
}
