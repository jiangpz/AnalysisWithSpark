package advanced.chapter6;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class LSA {

	public static void main(String[] args) {
		//初始化SparkConf
		SparkConf sc = new SparkConf().setMaster("local").setAppName("AnomalyDetectionInNetworkTraffic");
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		JavaSparkContext jsc = new JavaSparkContext(sc);

		//读入数据
		JavaRDD<String> rawData =jsc.textFile("src/main/java/advanced/chapter5/kddcup.data/kddcup.data.corrected");

		//查看有哪些类别标号及每类样本有多少
		ArrayList<Entry<String, Long>> lineList = new ArrayList<>(rawData.map(line -> line.split(",")[line.split(",").length-1]).countByValue().entrySet());
		Collections.sort(lineList, (m1, m2) -> m2.getValue().intValue()-m1.getValue().intValue());
		lineList.forEach(line -> System.out.println(line.getKey() + "," + line.getValue()));
		
		//删除下标从1开始的三个类别型列和最后的标号列
		JavaRDD<Tuple2<String, Vector>> labelsAndData = rawData.map(line -> {
			String[] lineArrya = line.split(",");
			double[] vectorDouble = new double[lineArrya.length-4];
			for (int i = 0, j=0; i < lineArrya.length; i++) {
				if(i==1 || i==2 || i==3 || i==lineArrya.length-1) {
					continue;
				}
				vectorDouble[j] = Double.parseDouble(lineArrya[i]);
				j++;
			}
			String label = lineArrya[lineArrya.length-1];
			Vector vector = Vectors.dense(vectorDouble);
			return new Tuple2<String, Vector>(label,vector);
		});
		
		RDD<Vector> data = JavaRDD.toRDD(labelsAndData.map(f -> f._2));
		
		//聚类
		KMeans kmeans = new KMeans();
		KMeansModel model = kmeans.run(data);
		
		//聚类结果
		Arrays.asList(model.clusterCenters()).forEach(v -> System.out.println(v.toJson()));
		
		ArrayList<Entry<Tuple2<Integer, String>, Long>> clusterLabelCount = new ArrayList<Entry<Tuple2<Integer, String>, Long>>(labelsAndData.map( v -> {
			int cluster = model.predict(v._2);
			return new Tuple2<Integer, String>(cluster, v._1);
		}).countByValue().entrySet());
		
		Collections.sort(clusterLabelCount, (m1, m2) -> m2.getKey()._1-m1.getKey()._1);
		clusterLabelCount.forEach(t -> System.out.println(t.getKey()._1 +"\t"+ t.getKey()._2 +"\t\t"+ t.getValue()));
		
		//选择K
		List<Double> list = Arrays.asList(new Integer[]{1, 2, 3, 4, 5, 6, 7, 8}).stream().map(k -> clusteringScore(labelsAndData.map(f -> f._2), k*5)).collect(Collectors.toList());
		list.forEach(System.out::println);
		
		KMeans kmeansF = new KMeans();
		kmeansF.setK(150);
		KMeansModel modelF = kmeansF.run(data);
		
		System.out.println("json:---------");
		Arrays.asList(modelF.clusterCenters()).forEach(v -> System.out.println(v.toJson()));
		
		ArrayList<Entry<Tuple2<Integer, String>, Long>> clusterLabelCountF = new ArrayList<Entry<Tuple2<Integer, String>, Long>>(labelsAndData.map( v -> {
			int cluster = modelF.predict(v._2);
			return new Tuple2<Integer, String>(cluster, v._1);
		}).countByValue().entrySet());
		
		Collections.sort(clusterLabelCountF, (m1, m2) -> m2.getKey()._1-m1.getKey()._1);
		clusterLabelCountF.forEach(t -> System.out.println(t.getKey()._1 +"\t"+ t.getKey()._2 +"\t\t"+ t.getValue()));
		
		//距离中心最远的第100个点的距离
		JavaDoubleRDD distances = labelsAndData.map(f -> f._2).mapToDouble(datum -> distToCentroid(datum, modelF));
		Double threshold = distances.top(100).get(99);
		
		JavaRDD<Tuple2<String, Vector>> result = labelsAndData.filter(t -> distToCentroid(t._2, modelF) > threshold);
		System.out.println("result:---------");
		result.foreach(f -> System.out.println(f._2));
		
		jsc.close();
		
	}
	
	/**
	 * 
	 * @Title: distance
	 * @Description: 计算两点距离
	 * @param: @param a
	 * @param: @param b
	 * @throws:
	 */
	public static double distance(Vector a, Vector b){
		double[] aArray = a.toArray();
		double[] bArray = b.toArray();
		ArrayList<Tuple2<Double, Double>> ab = new ArrayList<Tuple2<Double, Double>>();
		for (int i = 0; i < a.toArray().length; i++) {
			ab.add(new Tuple2<Double, Double>(aArray[i],bArray[i]));
		}
		return Math.sqrt(ab.stream().map(x -> x._1-x._2).map(d -> d*d).reduce((r,e) -> r= r+e).get());
	}
	
	/**
	 * 
	 * @Title: distToCentroid
	 * @Description: 计算数据点到簇质心距离
	 * @param: @param datum
	 * @return 
	 * @throws:
	 */
	public static double distToCentroid(Vector datum, KMeansModel model) {
		int cluster = model.predict(datum);
		 Vector[] centroid = model.clusterCenters();
		 return distance(centroid[cluster], datum);
	}
	
	public static double clusteringScore(JavaRDD<Vector> data, int k) {
		KMeans kmeans = new KMeans();
		kmeans.setK(k);
		KMeansModel model = kmeans.run(JavaRDD.toRDD(data));
		return data.mapToDouble(datum -> distToCentroid(datum, model)).stats().mean();
	}
}
