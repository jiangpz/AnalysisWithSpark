package advanced.chapter4;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.util.Utils;

import scala.Tuple2;

public class Forest {

	public static void main(String[] args) {
		//初始化SparkConf
		SparkConf sc = new SparkConf().setMaster("local").setAppName("PredictingForestCoverWithDecisionTrees");
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		JavaSparkContext jsc = new JavaSparkContext(sc);

		//读入数据
		JavaRDD<LabeledPoint> data = readData(jsc);
		
		//将数据分割为训练集、交叉检验集(CV)和测试集
		JavaRDD<LabeledPoint>[] splitArray = data.randomSplit(new double[]{0.8, 0.1, 0.1});
		JavaRDD<LabeledPoint> trainData = splitArray[0];
		trainData.cache();
		JavaRDD<LabeledPoint> cvData = splitArray[1];
		cvData.cache();
		JavaRDD<LabeledPoint> testData = splitArray[2];
		testData.cache();
		
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		map.put(10, 4);
		map.put(11, 40);
		//构建RandomForestModel
		RandomForestModel model = RandomForest.trainClassifier(trainData, 7, map, 20, "auto", "entropy", 30, 300, Utils.random().nextInt());
		//用CV集来计算结果模型的指标
		MulticlassMetrics metrics = getMetrics(model, cvData);
		System.out.println(metrics.precision());
		
		double[] input = new double[] {(double) 2709,(double) 125,(double) 28,(double) 67,(double) 23,(double) 3224,(double) 253,(double) 207,(double) 61,(double) 6094,(double) 0,(double) 29};
		Vector vector = Vectors.dense(input);
		System.out.println(model.predict(vector));
		
		jsc.close();
	}

	/**
	 * 
	 * @Title: readData
	 * @Description: 读取数据，转换为包含特征值和标号的特征向量
	 * @param: @param jsc
	 * @throws:
	 */
	public static JavaRDD<LabeledPoint> readData(JavaSparkContext jsc) {
		JavaRDD<String> rawData =jsc.textFile("src/main/java/advanced/chapter4/covtype/covtype.data");
		
		JavaRDD<LabeledPoint> data = rawData.map(line -> {
			String[] values = line.split(",");
			double[] features = new double[12];
			for (int i = 0; i < 10; i++) {
				features[i] = Double.parseDouble(values[i]);
			}
			for (int i = 10; i < 14; i++) {
				if(Double.parseDouble(values[i]) == 1.0){
					features[10] = i-10;
				}
			}
			for (int i = 14; i < 54; i++) {
				if(Double.parseDouble(values[i]) == 1.0){
					features[11] = i-14;
				}
			}
			Vector featureVector = Vectors.dense(features);
			Double label = (double) (Double.parseDouble(values[values.length-1]) - 1);
			return new LabeledPoint(label, featureVector);
		});
		return data;
	}
	
	/**
	 * 
	 * @Title: getMetrics
	 * @Description: 用CV集来计算结果模型的指标
	 * @param: @param model
	 * @param: @param data
	 * @throws:
	 */
	public static MulticlassMetrics getMetrics(RandomForestModel model, JavaRDD<LabeledPoint> data){
		JavaPairRDD<Object, Object> predictionsAndLabels = data.mapToPair(example -> {
			return new Tuple2<Object, Object>(model.predict(example.features()), example.label());
		});
		
		return new MulticlassMetrics(JavaPairRDD.toRDD(predictionsAndLabels));
	}
}