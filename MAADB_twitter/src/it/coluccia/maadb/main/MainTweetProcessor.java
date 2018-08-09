package it.coluccia.maadb.main;

import java.awt.Color;
import java.awt.Dimension;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.commons.io.FileUtils;

import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.PixelBoundryBackground;
import com.kennycason.kumo.font.scale.LinearFontScalar;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.palette.ColorPalette;

import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import edu.stanford.nlp.util.StringUtils;
import it.coluccia.maadb.dataaccess.MongoDBDAO;
import it.coluccia.maadb.dataaccess.OracleDAO;
import it.coluccia.maadb.datamodel.Emoji;
import it.coluccia.maadb.datamodel.Emoticon;
import it.coluccia.maadb.datamodel.HashTag;
import it.coluccia.maadb.datamodel.Tweet;
import it.coluccia.maadb.utils.Constants;
import it.coluccia.maadb.utils.MongoCollection;
import it.coluccia.maadb.utils.SentimentEnum;

public class MainTweetProcessor {

	private static final String TWEET_PATH = System.getProperty("user.dir") + "/resources/Tweets/";
	private static final String WORDS_CLOUDS_PATH = System.getProperty("user.dir") + "/resources/wordsClouds/";
	private static final String WORDS_CLOUD_TEMPLATE_IMAGE = "duke_resized_template.png";
	
	public static final int EMOJI_LIMIT = 50;
	public static final int EMOTICON_LIMIT = 50;
	public static final int HASHTAG_LIMIT = 50;
	public static final int TWEET_LIMIT = 200;
	
	private static String jdbcUrl = null;
	private static String usernameOracle = null;
	private static String passwordOracle = null;
	
	private static String mongoHost = null;
	private static int mongoPort = 0;
	private static String mongoUser = null;
	private static String mongoPassword = null;
	private static String mongoDbName = null;
	
	private static MongoDBDAO mongoDbDao = null;

	/**
	 * args[0] = jdbcurl. jdbc:oracle:thin:@localhost:1521:SID args[1]
	 * =oracleusername args[2] = oraclepassword args[3] = sentimentCode; args[4] = persistMode; args[5] = mongohost;
	 * args[6] = mongoUser; args[7] = mongoPassword; args[8] = mongoDbName; args[9] = mongoPort;
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length != 10) {
			System.out.println("!!!! YOU MUST PASS 4 PARAMETERS --> ABORT !!!!");
			System.exit(1);
		}

		jdbcUrl = args[0];
		usernameOracle = args[1];
		passwordOracle = args[2];
		
		
		
		Integer sentimentCode = Integer.parseInt(args[3]);
		int persistMode = Integer.parseInt(args[4]);//1=ORACLE;2=MONGODB;3=ALL
		
		mongoHost = args[5];
		mongoUser = args[6];
		mongoPassword = args[7];
		mongoDbName = args[8];
		mongoPort = Integer.parseInt(args[9]);

		if (!SentimentEnum.getIds().contains(sentimentCode)) {
			throw new IllegalArgumentException("!!!! The sentimentID passed is not valid --> ABORT !!!!");
		}
		
		if(persistMode != 1 && persistMode != 2 && persistMode != 3){
			throw new IllegalArgumentException("!!!! The persistMode passed is not valid --> ABORT !!!!");
		}
		
		if(persistMode == 2 || persistMode == 3){
			System.out.println("############## TRUNCATING MONGO COLLECTIONS ##############");
			mongoDbDao = new MongoDBDAO(mongoHost, mongoPort, mongoUser, mongoPassword, mongoDbName);
			mongoDbDao.truncateCollections();
		}

		SentimentEnum sentimentChosed = SentimentEnum.getSentimentFromId(sentimentCode);

		System.out.println("############## TWEET PROCESS STARTED : " + sentimentChosed + " ##############");

		try {
			String fileString = FileUtils.readFileToString(new File(TWEET_PATH + sentimentChosed.getFileName()),
					StandardCharsets.UTF_8);
			String[] tweetSentences = fileString.split("\\n");
			for (String sentence : tweetSentences) {
				System.out.println("---------- STARTING PIPELINE FOR TWEET : " + sentence);
				executePipeline(sentence, sentimentChosed,persistMode);
			}
			if(persistMode == 2 || persistMode == 3){
				mongoDbDao.executeMapReduce(MongoCollection.TWEETS,MongoCollection.TWEETS_REDUCED);
				mongoDbDao.executeMapReduce(MongoCollection.HASHTAGS,MongoCollection.HASHTAGS_REDUCED);
				mongoDbDao.executeMapReduce(MongoCollection.EMOTICONS,MongoCollection.EMOTICONS_REDUCED);
				mongoDbDao.executeMapReduce(MongoCollection.EMOJI,MongoCollection.EMOJI_REDUCED);
			}
			
			// TODO: filterResult(sentimentChosed,persistMode);
			generateWordsCloud(sentimentChosed,persistMode);
		} catch (Exception e) {
			System.out.println("!!!! ERROR OCCURRED --> ABORT !!!!");
			e.printStackTrace();
		}

		System.out.println("############## TWEET PROCESS COMPLETED : " + sentimentChosed + " ##############");
	}

	private static void executePipeline(String sentence, SentimentEnum sentiment,int persistMode) throws Exception {
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
		lemmas = stopWordsDeletion(lemmas);
		lemmas = puntualizationDeletion(lemmas);
		persist(lemmas,hashTags,emoji,emoticons,sentiment,persistMode);

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
			hashTags.add(mat.group());
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

		Pattern unicodeOutliers = Pattern.compile(
				"[\ud83c\udc00-\ud83c\udfff]|[\ud83d\udc00-\ud83d\udfff]|[\u2600-\u27ff]",
				Pattern.UNICODE_CASE | Pattern.CANON_EQ | Pattern.CASE_INSENSITIVE);

		Matcher unicodeOutlierMatcher = unicodeOutliers.matcher(utf8tweet);

		while (unicodeOutlierMatcher.find()) {
			emoji.add(unicodeOutlierMatcher.group());
		}

		utf8tweet = unicodeOutlierMatcher.replaceAll("");

		System.out.println("STEP4-END: processEmoji. SENTENCE AFTER: " + utf8tweet);

		return utf8tweet;
	}

	private static List<List<String>> tokenize(String sentence) {
		System.out.println("STEP5-START: tokenize. SENTENCE BEFORE: " + sentence);

		List<List<String>> result = new ArrayList<>();

		Document doc = new Document(sentence);

		for (Sentence sent : doc.sentences()) {
			List<String> tokenizedSentence = new ArrayList<>();
			tokenizedSentence.addAll(sent.words());
			result.add(tokenizedSentence);
		}

		System.out.println("STEP5-END: tokenize. Number of tokens: " + result.size());

		return result;
	}

	private static List<List<String>> substituteSlangAndAcronims(List<List<String>> tokenizedSentences)
			throws IOException {
		System.out.println("STEP6-START: substituteSlangAndAcronims");

		int numberSubstitution = 0;
		List<List<String>> result = new ArrayList<>();

		JsonReader reader = Json.createReader(new FileInputStream(
				new File(System.getProperty("user.dir") + "/resources/nlpResources/slangWords.txt")));
		JsonObject slangWords = reader.readObject();
		reader.close();

		for (List<String> tokens : tokenizedSentences) {
			List<String> cleanedTokenizedSentence = new ArrayList<>();
			for (String token : tokens) {
				if (slangWords.get(token) != null) {
					numberSubstitution++;
					cleanedTokenizedSentence.add(slangWords.get(token).toString().replaceAll("\"", ""));
				} else {
					cleanedTokenizedSentence.add(token);
				}
			}
			result.add(cleanedTokenizedSentence);

		}

		System.out.println("STEP6-END: substituteSlangAndAcronims; Substituted tokens: " + numberSubstitution);

		return result;
	}

	private static List<String> lemmatization(List<List<String>> tokenizedSentences) {
		System.out.println("STEP7-START: lemmatization");

		List<String> lemmas = new ArrayList<>();

		for (List<String> tokenizedSentence : tokenizedSentences) {
			Sentence sentence = new Sentence(tokenizedSentence);
			lemmas.addAll(sentence.lemmas());
		}

		System.out.println("STEP7-END: lemmatization");

		return lemmas;
	}

	private static List<String> puntualizationDeletion(List<String> lemmas) {
		System.out.println("STEP9-START: puntalizationDeletion");

		String[] puntualizationMarks = new String[] { ",", "?", "!", ".", ";", ":", "\\", "/", "(", ")", "&", "_", "+",
				"=", "<>", "\"" };
		Set<String> puntualizationMarksSet = Arrays.stream(puntualizationMarks).collect(Collectors.toSet());

		List<String> result = new ArrayList<>();

		for (String lemma : lemmas) {
			if (!puntualizationMarksSet.contains(lemma)) {
				result.add(lemma);
			}
		}

		System.out.println("STEP9-END: puntalizationDeletion");

		return result;
	}

	private static List<String> stopWordsDeletion(List<String> lemmas) {
		System.out.println("STEP8-START: stopWordsDeletion");

		Set<String> stopWordsSet = Arrays.stream(Constants.STOP_WORDS_ARRAY).collect(Collectors.toSet());

		List<String> result = new ArrayList<>();
		
		int numberDeletion = 0;

		for (String lemma : lemmas) {
			if (!stopWordsSet.contains(lemma)) {
				result.add(lemma);
			}
			else{
				numberDeletion++;
			}
		}

		System.out.println("STEP8-END: stopWordsDeletion; Deleted tokens: " + numberDeletion);

		return result;
	}

	private static void persist(List<String> lemmas,List<String> hashTags,List<String> emoji,List<String> emoticons, SentimentEnum sentiment,int persistMode) throws Exception{
		System.out.println("STEP10-START: persist");
		
		if(lemmas != null && !lemmas.isEmpty()){
			if(persistMode == 1){
				OracleDAO oracleDao = new OracleDAO(jdbcUrl,usernameOracle,passwordOracle);
			    oracleDao.persist(lemmas,hashTags,emoji,emoticons,sentiment);
			}
			else if(persistMode == 2){
				//MongoDBDAO mongoDbDao = new MongoDBDAO(mongoHost, mongoPort, mongoUser, mongoPassword, mongoDbName);
				mongoDbDao.persist(lemmas,hashTags,emoji,emoticons,sentiment);
			}
			else if(persistMode == 3){
				OracleDAO oracleDao = new OracleDAO(jdbcUrl,usernameOracle,passwordOracle);
				//MongoDBDAO mongoDbDao = new MongoDBDAO(mongoHost, mongoPort, mongoUser, mongoPassword, mongoDbName);
				
				oracleDao.persist(lemmas,hashTags,emoji,emoticons,sentiment);
				mongoDbDao.persist(lemmas,hashTags,emoji,emoticons,sentiment);
			}
			else{
				throw new IllegalArgumentException("!!! PersistMode parameter not valid --> ABORT!");
			}
		}
		
		System.out.println("STEP10-END: persist");
	}
	
	
	private static void generateWordsCloud(SentimentEnum sentiment,int persistMode) throws IOException, SQLException{
		System.out.println("###### GENERATING WORLD CLOUDS ######");
		
		final FrequencyAnalyzer frequencyAnalyzer = new FrequencyAnalyzer();
		frequencyAnalyzer.setWordFrequenciesToReturn(300);
		frequencyAnalyzer.setMinWordLength(2);
		
		List<WordFrequency> wordFrequenciesOracle = null;
		List<WordFrequency> wordFrequenciesMongo = null;

		if(persistMode == 1){
			wordFrequenciesOracle = generateListWordFrequenciesOracle(sentiment);
		}
		else if(persistMode == 2){
			wordFrequenciesMongo = generateListWordFrequenciesMongo(sentiment);
		}
		else{
			//persistMode == 3
			wordFrequenciesOracle = generateListWordFrequenciesOracle(sentiment);
			wordFrequenciesMongo = generateListWordFrequenciesMongo(sentiment);
		}
		final Dimension dimension = new Dimension(500, 312);
		final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
		wordCloud.setPadding(2);
		wordCloud.setBackground(new PixelBoundryBackground(WORDS_CLOUDS_PATH + "/templateImages" + WORDS_CLOUD_TEMPLATE_IMAGE));
		wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
		wordCloud.setFontScalar(new LinearFontScalar(10, 40));
		
		String toDateString = new Date().toString();
		
		if(persistMode == 1){
			wordCloud.build(wordFrequenciesOracle);
			wordCloud.writeToFile(WORDS_CLOUDS_PATH + "/outputWordClouds/Oracle/"+toDateString+"/"+sentiment.name());
		}
		else if(persistMode == 2){
			wordCloud.build(wordFrequenciesMongo);
			wordCloud.writeToFile(WORDS_CLOUDS_PATH + "/outputWordClouds/Mongo/"+toDateString+"/"+sentiment.name());
		}
		else{
			//persistMode == 3
			wordCloud.build(wordFrequenciesOracle);
			wordCloud.writeToFile(WORDS_CLOUDS_PATH + "/outputWordClouds/Oracle/"+toDateString+"/"+sentiment.name());
			wordCloud.build(wordFrequenciesMongo);
			wordCloud.writeToFile(WORDS_CLOUDS_PATH + "/outputWordClouds/Mongo/"+toDateString+"/"+sentiment.name());
		}
		
		System.out.println("###### WORLD CLOUDS GENERATED ######");
	}
	
	private static List<WordFrequency> generateListWordFrequenciesOracle(SentimentEnum sentiment) throws SQLException{
		OracleDAO oracleDao = new OracleDAO(jdbcUrl,usernameOracle,passwordOracle);
		List<Emoji> mostFrequentEmoji = oracleDao.getMostFreqEmoji();
		List<Emoticon> mostFrequentEmoticon = oracleDao.getMostFreqEmoticon();
		List<HashTag> mostFrequentHashTag = oracleDao.getMostFreqHashTag();
		List<Tweet>  mostFrequentTweet = oracleDao.getMostFreqTweet();
		
		List<WordFrequency> result = new ArrayList<>();
		int counter = 0;
		for(Emoji emoji : mostFrequentEmoji){
			if(counter > EMOJI_LIMIT){
				break;
			}
			WordFrequency item = new WordFrequency(emoji.getWord(),emoji.getFrequency());
			result.add(item);
		}
		for(Emoticon emoticon : mostFrequentEmoticon){
			if(counter > EMOTICON_LIMIT){
				break;
			}
			WordFrequency item = new WordFrequency(emoticon.getWord(),emoticon.getFrequency());
			result.add(item);
		}
		for(HashTag hashTag : mostFrequentHashTag){
			if(counter > HASHTAG_LIMIT){
				break;
			}
			WordFrequency item = new WordFrequency(hashTag.getWord(),hashTag.getFrequency());
			result.add(item);
		}
		for(Tweet tweet : mostFrequentTweet){
			if(counter > TWEET_LIMIT){
				break;
			}
			WordFrequency item = new WordFrequency(tweet.getWord(),tweet.getFrequency());
			result.add(item);
		}
		
		return result;
	}
	
	private static List<WordFrequency> generateListWordFrequenciesMongo(SentimentEnum sentiment) throws SQLException{
		List<Emoji> mostFrequentEmoji = mongoDbDao.getMostFreqEmoji();
		List<Emoticon> mostFrequentEmoticon = mongoDbDao.getMostFreqEmoticon();
		List<HashTag> mostFrequentHashTag = mongoDbDao.getMostFreqHashTag();
		List<Tweet>  mostFrequentTweet = mongoDbDao.getMostFreqTweet();
		
		List<WordFrequency> result = new ArrayList<>();
		int counter = 0;
		for(Emoji emoji : mostFrequentEmoji){
			if(counter > EMOJI_LIMIT){
				break;
			}
			WordFrequency item = new WordFrequency(emoji.getWord(),emoji.getFrequency());
			result.add(item);
		}
		for(Emoticon emoticon : mostFrequentEmoticon){
			if(counter > EMOTICON_LIMIT){
				break;
			}
			WordFrequency item = new WordFrequency(emoticon.getWord(),emoticon.getFrequency());
			result.add(item);
		}
		for(HashTag hashTag : mostFrequentHashTag){
			if(counter > HASHTAG_LIMIT){
				break;
			}
			WordFrequency item = new WordFrequency(hashTag.getWord(),hashTag.getFrequency());
			result.add(item);
		}
		for(Tweet tweet : mostFrequentTweet){
			if(counter > TWEET_LIMIT){
				break;
			}
			WordFrequency item = new WordFrequency(tweet.getWord(),tweet.getFrequency());
			result.add(item);
		}
		
		return result;
	}

}
