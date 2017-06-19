package advanced.chapter6;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
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

		//读入数据,在执行前需要先执行WriteToHadoop
		String path = "/user/ds/wikidump.xml";
		Configuration conf = new Configuration();
		conf.set(XMLInputFormat.START_TAG_KEY, "<page>");
		conf.set(XMLInputFormat.END_TAG_KEY, "</page>");
		JavaPairRDD<LongWritable, Text> kvs = jsc.newAPIHadoopFile(path, XMLInputFormat.class, LongWritable.class, Text.class, conf);
		JavaRDD<String> rawXmls = kvs.map(p -> p.toString());
//		JavaRDD<String> plainText = rawXmls.filter(x -> { return x != null; }).flatMap(LSA::wikiXmlToPlainText);
//		JavaRDD<Tuple2<String, String>> plainText = rawXmls.flatMap(LSA::wikiXmlToPlainText);
		JavaRDD<Page> plainText = rawXmls.flatMap(LSA::wikiXmlToPlainText);
//		plainText.foreach(x -> System.out.println(x.tittle));
//		TODO 读取文件存在错误
		
		//词形归并
		HashSet<String> stopWords = jsc.broadcast(loadStopWords("/stopwords.txt")).value();
		JavaRDD<Lemmas> lemmatized = plainText.mapPartitions(iter -> {
			StanfordCoreNLP pipeline = createNLPPipeline();
//			return plainTextToLemmas(iter.next().content, stopWords, pipeline);
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
		
		//TF-IDF 2.1 计算文档频率(非分布式方式)
//		docTermFreqs.aggregate(new HashMap<String, Integer>(), seqOp, combOp);
		
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
//		docFreqs.foreach(x -> System.out.println(x));
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
//		bTermIds.foreach(x -> System.out.println(x));

		//TF-IDF 3.为每个文档建立一个含权重TF-IDF向量，稀疏矩阵
		JavaRDD<Vector> vecs = docTermFreqs.map(termFreqs -> {
			Integer docTotalTerms = termFreqs._2.values().stream().reduce((x, y) -> x + y).get();//一个文章中共有多少词
			
//			Iterator<Entry<Long, Double>> termScores = termFreqs._2.entrySet().stream().filter(x -> bTermIds.containsKey(x))
//					.collect(Collectors.toMap(e -> bTermIds.get(e.getKey()),
//							e -> bIdfs.get(e.getKey()) * termFreqs._2.get(e.getKey()) / docTotalTerms)).entrySet().iterator();
			
			List<Tuple2<Integer, Double>> termScores = termFreqs._2.entrySet().stream().filter(x -> bTermIds.containsKey(x)).map(e -> {
				return new Tuple2<Integer, Double>(bTermIds.get(e.getKey()).intValue(), bIdfs.get(e.getKey()) * termFreqs._2.get(e.getKey()) / docTotalTerms);
			}).collect(Collectors.toList());
			
			return Vectors.sparse(bTermIds.size(), termScores);
		});
		
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
		if (page.isEmpty()) {
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
}
	
