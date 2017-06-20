package advanced.chapter6;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import advanced.chapter6.entity.Lemmas;
import advanced.chapter6.entity.Page;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import edu.umd.cloud9.collection.XMLInputFormat;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;
import scala.Tuple2;

public class LSA {

	static Logger logger = LoggerFactory.getLogger(LSA.class);
	
	public static void main(String[] args) {
		//初始化SparkConf
		SparkConf sc = new SparkConf().setMaster("local").setAppName("Wiki LSA");
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		JavaSparkContext jsc = new JavaSparkContext(sc);
//		jsc.setLogLevel("ERROR");

		//读入数据,在执行前需要先执行WriteToHadoop
		String path = "/user/ds/wikidump.xml";
		Configuration conf = new Configuration();
		conf.set(XMLInputFormat.START_TAG_KEY, "<page>");
		conf.set(XMLInputFormat.END_TAG_KEY, "</page>");
		JavaPairRDD<LongWritable, Text> kvs = jsc.newAPIHadoopFile(path, XMLInputFormat.class, LongWritable.class, Text.class, conf);
		JavaRDD<String> rawXmls = kvs.map(p -> p.toString());
//		JavaRDD<Tuple2<String, String>> plainText = rawXmls.flatMap(LSA::wikiXmlToPlainText);
		JavaRDD<Page> plainText = rawXmls.filter(x -> { return x != null; }).flatMap(LSA::wikiXmlToPlainText);
//		plainText.foreach(x -> System.out.println(x.tittle));
//		TODO 读取文件存在错误
		
		//词形归并
		HashSet<String> stopWords = jsc.broadcast(loadStopWords("/stopwords.txt")).value();
		JavaRDD<Lemmas> lemmatized = plainText.mapPartitions(iter -> {
			StanfordCoreNLP pipeline = createNLPPipeline();
			ArrayList<Lemmas> lemmasList = new ArrayList<Lemmas>();
			lemmasList.add(new Lemmas(iter.next().tittle,plainTextToLemmas(iter.next().content, stopWords, pipeline)));
			return lemmasList;
		});
		
		JavaRDD<Lemmas> filteredLemmatized = lemmatized.filter(x -> x.lemmas.size() > 1);
		
		//TF-IDF 1.每个文档的词项频率的映射 （文章名-词项-数量）
		JavaRDD<Tuple2<String, HashMap<String, Integer>>> docTermFreqs = filteredLemmatized.map(terms -> {
			String tittle = terms.tittle;
			ArrayList<String> lemmas = terms.lemmas;
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			for (int i = 0; i < lemmas.size(); i++) {
				String lemma = lemmas.get(i);
				if (map.containsKey(lemma)) {
					map.put(lemma, map.get(lemma) + 1);
				} else {
					map.put(lemma, 1);
				}
			}
			return new Tuple2<String, HashMap<String, Integer>> (tittle, map);
		});
		docTermFreqs.cache();
		Long numDocs = docTermFreqs.count();
		System.out.println("文档个数：" + numDocs);
		
		//查看有多少个词项
		long count = docTermFreqs.flatMap(x -> {
			return x._2.keySet();
		}).distinct().count();
		System.out.println("词项个数：" + count);
		
		//TF-IDF 2.1 计算文档频率(非分布式方式,使用aggregate,适合数据量较小的时候,在此略过)
		//TF-IDF 2.2 计算文档频率(分布式方式)
		//文档每出现一个不同的词项，程序就生成一个由词项和数字1 组成的键-值对
		JavaPairRDD<String, Integer> docFreqs = docTermFreqs.flatMap(x -> x._2.keySet()).mapToPair(y -> {
			return new Tuple2<String, Integer>(y, 1);
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		//docFreqs.foreach(x -> System.out.println(x));
		//DF 前1500个词的文档频率
		Integer numTerms = 1500;
		JavaPairRDD<Integer, String> orderingSwap = docFreqs.mapToPair(x -> x.swap()).sortByKey(false);
		List<Tuple2<Integer, String>> topDocFreqs = orderingSwap.take(numTerms);
		//IDF
		JavaPairRDD<String, Double> idfs = docFreqs.mapToPair(x -> {
			return new Tuple2<String, Double>(x._1, Math.log(numDocs.doubleValue() / x._2));
		});
		HashMap<String, Double> bIdfs = new HashMap<String, Double> (idfs.collectAsMap());
		//为每个词项分配ID
		JavaPairRDD<String, Long> termIds = idfs.keys().zipWithIndex();
		Map<String, Long> bTermIds = jsc.broadcast(termIds).value().collectAsMap();
		JavaPairRDD<Long, String> termIdsSwap = termIds.mapToPair(x -> x.swap());
		Map<Long, String> bTermIdsSwap = jsc.broadcast(termIdsSwap).value().collectAsMap();
		//bTermIds.foreach(x -> System.out.println(x));

		//TF-IDF 3.为每个文档建立一个含权重TF-IDF向量，稀疏矩阵
		JavaRDD<Vector> vecs = docTermFreqs.map(termFreqs -> {
			Integer docTotalTerms = termFreqs._2.values().stream().reduce((x, y) -> x + y).get();//一个文章中共有多少词
			
			List<Tuple2<Integer, Double>> termScores = termFreqs._2.entrySet().stream().filter(x -> bTermIds.containsKey(x.getKey())).map(e -> {
				return new Tuple2<Integer, Double>(bTermIds.get(e.getKey()).intValue(), bIdfs.get(e.getKey()) * termFreqs._2.get(e.getKey()) / docTotalTerms);
			}).collect(Collectors.toList());
			
			return Vectors.sparse(bTermIds.size(), termScores);
		});
		//vecs.foreach(x -> System.out.println(x.toString()));
		
		//将行向量RDD 包装为RowMatrix 并调用computeSVD计算SVD
		RDD<Vector> termDocMatrix = JavaRDD.toRDD(vecs);
		termDocMatrix.cache();
		RowMatrix mat = new RowMatrix(termDocMatrix);
		int k = 1000;
		boolean computeU = true;
		double rCond = 1e-9;
		SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(k, computeU, rCond);//20分钟
		
		//A≈USV
		System.out.println("Singular values: " + svd.s());
		Matrix v = svd.V();
		double[] arr = v.toArray();
		for (int i = 0; i < arr.length; i++) {
			System.out.println(arr[i]);
		}
		
		jsc.close();
	}
	
	/**
	 * 
	 * @Title: wikiXmlToPlainText
	 * @Description: 将维基百科的XML 文件转成纯文本
	 * @param: @param xml
	 * @param: @return    参数
	 * @return: List<String>    返回类型
	 * @throws:
	 */
	public static ArrayList<Page> wikiXmlToPlainText(String xml) {
		EnglishWikipediaPage page = new EnglishWikipediaPage();
		WikipediaPage.readPage(page, xml);
		ArrayList<Page> pageList = new ArrayList<Page>();
		if (page.isEmpty() || !page.isArticle() || page.isRedirect() || page.getTitle().contains("(disambiguation)")) {
			return null;
		} else {
			Page pageEntity = new Page(page.getTitle(),page.getContent());
			pageList.add(pageEntity);
			return pageList;
		}
	}
	
	/**
	 * 
	 * @Title: createNLPPipeline
	 * @Description: 词形归并
	 * @param: @return    参数
	 * @return: StanfordCoreNLP    返回类型
	 * @throws:
	 */
	public static StanfordCoreNLP createNLPPipeline() {
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");
		return new StanfordCoreNLP(props);
	}
	public static Boolean isOnlyLetters(String str) {
		Integer i = 0;
		while (i < str.length()) {
			if (!Character.isLetter(str.charAt(i))) {
				return false;
			}
			i += 1;
		}
		return true;
	}
	public static ArrayList<String> plainTextToLemmas(String text, Set<String> stopWords, StanfordCoreNLP pipeline) {
		Annotation doc = new Annotation(text);
		pipeline.annotate(doc);
		ArrayList<String> lemmas = new ArrayList<String>();
		List<CoreMap> sentences = doc.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {
			for (CoreMap token : sentence.get(TokensAnnotation.class)) {
				String lemma = token.get(LemmaAnnotation.class);
				if (lemma.length() > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
					lemmas.add(lemma.toLowerCase());
				}
			}
		}
		return lemmas;
	}
	public static HashSet<String> loadStopWords(String path) {
		HashSet<String> stopWordSet = new HashSet<String>();
		InputStream stopWordInputStream = LSA.class.getResourceAsStream(path);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stopWordInputStream));
		
		try {
			String stopWord = null;
			while((stopWord = bufferedReader.readLine()) != null)  
			{  
				stopWordSet.add(stopWord);
			}
		} catch (IOException e) {
			logger.error("读取停用词文件发生错误。" , e);
		}
		return stopWordSet;
	}
	
//	得到与最重要的概念最相关的词项
	public static ArrayList<Tuple2<String, Double>> topTermsInTopConcepts(SingularValueDecomposition<RowMatrix, Matrix> svd, Integer numConcepts, Integer numTerms, Map<Long, String> bTermIdsSwap) {
		Matrix v = svd.V();
		ArrayList<Tuple2<String, Double>> topTerms = new ArrayList<Tuple2<String, Double>>();
		double[] arr = v.toArray();
		for (int i = 0; i < numConcepts; i++) {
			int offs = i * v.numRows();
			ArrayList<Tuple2<Double, Integer>> termWeights = new ArrayList<Tuple2<Double, Integer>>();
			for (int j = 0; j < v.numRows(); j++) {
				termWeights.add(new Tuple2<Double, Integer>(arr[offs + j], j));
			}
			Collections.sort(termWeights, new Comparator<Tuple2<Double, Integer>>() {
				@Override
				public int compare(Tuple2<Double, Integer> o1, Tuple2<Double, Integer> o2) {
					if (o2._1-o1._1 > 0) {
						return 1;
					} else if (o2._1-o1._1 == 0){
						return 0;
					} else {
						return -1;
					}
				}
			});
			List<Tuple2<String, Double>> topTermsList = termWeights.subList(0, numTerms).stream().map(x -> {
				return new Tuple2<String, Double>(bTermIdsSwap.get(x._2.longValue()), x._1);
			}).collect(Collectors.toList());
			topTerms.addAll(topTermsList);
		}
		return topTerms;
	}
//	  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
//		      numTerms: Int, termIds: Map[Int, String]): Seq[Seq[(String, Double)]] = {
//		    val v = svd.V
//		    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
//		    val arr = v.toArray
//		    for (i <- 0 until numConcepts) {
//		      val offs = i * v.numRows
//		      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
//		      val sorted = termWeights.sortBy(-_._1)
//		      topTerms += sorted.take(numTerms).map{case (score, id) => (termIds(id), score)}
//		    }
//		    topTerms
//		  }
	public static ArrayList<Tuple2<String, Double>> topDocsInTopConcepts(SingularValueDecomposition<RowMatrix, Matrix> svd, Integer numConcepts, Integer numDocs, Map<Long, String> docIds) {
		RowMatrix u = svd.U();
		ArrayList<Tuple2<String, Double>> topDocs = new ArrayList<Tuple2<String, Double>>();
		for (int i = 0; i < numConcepts; i++) {
			Integer tmpI = i;
			JavaPairRDD<Double, Long> docWeights = u.rows().toJavaRDD().map(x -> x.toArray()[tmpI]).zipWithIndex();
			topDocs.addAll(docWeights.top(numDocs).stream().map(x -> new Tuple2<String, Double>(docIds.get(x._2), x._1)).collect(Collectors.toList()));
		}
		return topDocs;
	}
//		  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
//		      numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
//		    val u  = svd.U
//		    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
//		    for (i <- 0 until numConcepts) {
//		      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
//		      topDocs += docWeights.top(numDocs).map{case (score, id) => (docIds(id), score)}
//		    }
//		    topDocs
//		  }
}
	
