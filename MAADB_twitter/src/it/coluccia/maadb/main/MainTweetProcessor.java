package it.coluccia.maadb.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.commons.io.FileUtils;

import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import edu.stanford.nlp.util.StringUtils;
import it.coluccia.maadb.utils.Constants;
import it.coluccia.maadb.utils.SentimentEnum;

public class MainTweetProcessor {

	private static final String TWEET_PATH = System.getProperty("user.dir") + "/resources/Tweets/";

	/**
	 * args[0] = jdbcurl. jdbc:oracle:thin:@localhost:1521:SID args[1]
	 * =oracleusername args[2] = oraclepassword args[3] = sentimentCode
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length != 4) {
			System.out.println("!!!! YOU MUST PASS 4 PARAMETERS --> ABORT !!!!");
			System.exit(1);
		}

		String jdbcUrl = args[0];
		String username = args[1];
		String password = args[2];
		Integer sentimentCode = Integer.parseInt(args[3]);

		if (!SentimentEnum.getIds().contains(sentimentCode)) {
			throw new IllegalArgumentException("!!!! The sentimentID passed is not valid --> ABORT !!!!");
		}

		SentimentEnum sentimentChosed = SentimentEnum.getSentimentFromId(sentimentCode);

		System.out.println("############## TWEET PROCESS STARTED : " + sentimentChosed + " ##############");

		try {
			String fileString = FileUtils.readFileToString(new File(TWEET_PATH + sentimentChosed.getFileName()),
					StandardCharsets.UTF_8);
			String[] tweetSentences = fileString.split("\\n");
			for (String sentence : tweetSentences) {
				System.out.println("---------- STARTING PIPELINE FOR TWEET : " + sentence);
				executePipeline(sentence, sentimentChosed);
				generateWordsCloud(sentimentChosed);
			}
		} catch (IOException e) {
			System.out.println("!!!! ERROR OCCURRED --> ABORT !!!!");
			e.printStackTrace();
		}

		System.out.println("############## TWEET PROCESS COMPLETED : " + sentimentChosed + " ##############");
	}

	private static void executePipeline(String sentence, SentimentEnum sentiment) {
		if (StringUtils.isNullOrEmpty(sentence)) {
			System.out.println("Sentence empty --> SKIPPED!");
			return;
		}
		List<String> hashTags = new ArrayList<>();
		List<String> emoji = new ArrayList<>();
		List<String> emoticons = new ArrayList<>();

		sentence = cleanUserAndPwd(sentence);
		sentence = processHashtag(sentence, hashTags);
		sentence = processEmoticons(sentence, emoticons);
		sentence = processEmoji(sentence, emoji);
		sentence = sentence.toLowerCase();
		List<List<String>> tokenizedSentences = tokenize(sentence);
		tokenizedSentences = substituteSlangAndAcronims(tokenizedSentences);
		List<String> lemmas = lemmatization(tokenizedSentences);
		lemmas = puntualizationDeletion(lemmas);
		persist(lemmas, sentiment);

	}

	private static String cleanUserAndPwd(String sentence) {
		System.out.println("STEP1-START: cleanUserAndPwd. SENTENCE BEFORE: " + sentence);

		sentence = sentence.replaceAll("URL", "");
		sentence = sentence.replaceAll("USERNAME", "");

		System.out.println("STEP1-END: cleanUserAndPwd. SENTENCE AFTER: " + sentence);

		return sentence;
	}

	private static String processHashtag(String sentence, List<String> hashTags) {
		System.out.println("STEP2-START: processHashtag. SENTENCE BEFORE: " + sentence);

		Pattern hashtagPattern = Pattern.compile("#\\w+");
		Matcher mat = hashtagPattern.matcher(sentence);
		while (mat.find()) {
			hashTags.add(mat.group(1));
		}

		sentence = sentence.replaceAll("#\\w+", "");

		System.out.println("STEP2-END: processHashtag. SENTENCE AFTER: " + sentence);

		return sentence;
	}

	private static String processEmoticons(String sentence, List<String> emoticons) {
		System.out.println("STEP3-START: processEmoticons. SENTENCE BEFORE: " + sentence);

		Pattern emoticonsPattern = Pattern.compile(Constants.POSEMOTICONS_REGEX);
		Matcher mat = emoticonsPattern.matcher(sentence);
		while (mat.find()) {
			emoticons.add(mat.group());
		}

		emoticonsPattern = Pattern.compile(Constants.NEGEMOTICONS_REGEX);
		mat = emoticonsPattern.matcher(sentence);
		while (mat.find()) {
			emoticons.add(mat.group());
		}

		sentence = sentence.replaceAll(Constants.POSEMOTICONS_REGEX, "");
		sentence = sentence.replaceAll(Constants.NEGEMOTICONS_REGEX, "");

		System.out.println("STEP3-END: processEmoticons. SENTENCE AFTER: " + sentence);

		return sentence;
	}

	private static String processEmoji(String sentence, List<String> emoji) throws UnsupportedEncodingException {
		System.out.println("STEP4-START: processEmoji. SENTENCE BEFORE: " + sentence);

		byte[] utf8Bytes = sentence.getBytes("UTF-8");
		String utf8tweet = new String(utf8Bytes, "UTF-8");
		
	    Pattern unicodeOutliers =
		        Pattern.compile(
		            "[\ud83c\udc00-\ud83c\udfff]|[\ud83d\udc00-\ud83d\udfff]|[\u2600-\u27ff]",
		            Pattern.UNICODE_CASE |
		            Pattern.CANON_EQ |
		            Pattern.CASE_INSENSITIVE
		        );
	    
	    Matcher unicodeOutlierMatcher = unicodeOutliers.matcher(utf8tweet);
	    
	    while (unicodeOutlierMatcher.find()) {
	    	emoji.add(unicodeOutlierMatcher.group());
		}
	    
	    utf8tweet =unicodeOutlierMatcher.replaceAll("");

		System.out.println("STEP4-END: processEmoji. SENTENCE AFTER: " + utf8tweet);

		return utf8tweet;
	}
	
	private static List<List<String>> tokenize(String sentence){
		System.out.println("STEP5-START: tokenize. SENTENCE BEFORE: " + sentence);
		
		List<List<String>> result = new ArrayList<>();
		
		Document doc = new Document(sentence);
		
		for (Sentence sent : doc.sentences()){
			List<String> tokenizedSentence = new ArrayList<>();
			tokenizedSentence.addAll(sent.words());
			result.add(tokenizedSentence);
		}
		
		System.out.println("STEP5-END: tokenize. Number of tokens: " + result.size());
		
		return result;
	}
	
	private static List<List<String>> substituteSlangAndAcronims(List<List<String>> tokenizedSentences) throws IOException{
		System.out.println("STEP6-START: substituteSlangAndAcronims");
		
		int numberSubstitution = 0;
		List<List<String>> result = new ArrayList<>();
		
		JsonReader reader = Json.createReader(new FileInputStream(new File(System.getProperty("user.dir")+"/resources/nlpResources/slangWords.txt")));
        JsonObject slangWords = reader.readObject();
        reader.close();
        
        for(List<String> tokens : tokenizedSentences ){
        	List<String> cleanedTokenizedSentence = new ArrayList<>();
        	for(String token :  tokens){
            	if(slangWords.get(token) != null){
            		numberSubstitution++;
            		cleanedTokenizedSentence.add(slangWords.get(token).toString());
            	}
            	cleanedTokenizedSentence.add(token);
            }
        	result.add(cleanedTokenizedSentence);
	
        }
		
		System.out.println("STEP6-END: substituteSlangAndAcronims; Substituted tokens: "+numberSubstitution);
		
		return result;
	}
	
	private static List<String> lemmatization(List<List<String>> tokenizedSentences){
		System.out.println("STEP7-START: lemmatization");
		
		List<String> lemmas = new ArrayList<>();
		
		for(List<String> tokenizedSentence : tokenizedSentences){
			Sentence sentence = new Sentence(tokenizedSentence);
			lemmas.addAll(sentence.lemmas());
		}
		
		System.out.println("STEP7-END: lemmatization");
		
		return lemmas;
	}
	
	private static List<String> puntualizationDeletion(List<String> lemmas){
		System.out.println("STEP8-START: puntalizationDeletion");
		
		String[] puntualizationMarks = new String[]{",","?","!",".",";",":","\\","/","(",")","&","_","+","=","<>","\""};
		Set<String> puntualizationMarksSet = Arrays.stream(puntualizationMarks).collect(Collectors.toSet());
		
		List<String> result = new ArrayList<>();
		
		for(String lemma : lemmas){
			if(!puntualizationMarksSet.contains(lemma)){
				result.add(lemma);
			}
		}
		
		System.out.println("STEP8-END: puntalizationDeletion");
		
		return result;
	}

}
