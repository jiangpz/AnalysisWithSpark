package SparkLearn.SparkTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import advanced.chapter6.LSA;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 * 
 * @ClassName: StanfordCoreNLPTest
 * @Description: 分词
 * @author: 蒋佩釗
 * @date: 2017年6月16日 下午2:27:57
 *
 */
public class StanfordCoreNLPTest {
	
	static Logger logger = LoggerFactory.getLogger(StanfordCoreNLPTest.class);
			
	public static void main(String[] args) {
		String text = "These are tasks that simply could not be accomplished 5 or 10 years ago. accomplished";
		HashSet<String> stopWords = loadStopWords("/stopwords.txt");
		StanfordCoreNLP pipeline = createNLPPipeline();
		plainTextToLemmas(text, stopWords, pipeline).forEach(System.out::println);
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
}
