package it.coluccia.maadb.dataaccess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.ValidationOptions;

import it.coluccia.maadb.datamodel.Emoji;
import it.coluccia.maadb.datamodel.Emoticon;
import it.coluccia.maadb.datamodel.HashTag;
import it.coluccia.maadb.datamodel.Tweet;
import it.coluccia.maadb.main.MainTweetProcessor;
import it.coluccia.maadb.utils.Constants;
import it.coluccia.maadb.utils.MongoCollection;
import it.coluccia.maadb.utils.SentimentEnum;

public class MongoDBDAO {

	private MongoClient mongo;
	private MongoDatabase database;
	private String host;
	private int port;
	private String user;
	private String password;
	private String dbName;

	public MongoDBDAO(String host, int port, String user, String password, String dbName) {
		
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.dbName = dbName;
		
		// Creating Credentials
		MongoCredential credential = MongoCredential.createCredential(user, dbName, password.toCharArray());
		
		mongo = MongoClients.create(
		        MongoClientSettings.builder()
		                .applyToClusterSettings(builder -> 
		                        builder.hosts(Arrays.asList(new ServerAddress(host, port))))
		                .credential(credential)
		                .writeConcern(WriteConcern.UNACKNOWLEDGED)
		                .readPreference(ReadPreference.primaryPreferred())
		                .build());

		// Accessing the database
		database = mongo.getDatabase(dbName);
		System.out.println("MONGODB: Connected to the database successfully");
		
		checkCollectionsAndCreate();
	}
	
	public void persist(List<String> lemmas, List<String> hashTags, List<String> emoji, List<String> emoticons,
			SentimentEnum sentiment) throws Exception {
		insertLemmas(lemmas,sentiment);
		insertHashTags(hashTags,sentiment);
		insertEmoji(emoji,sentiment);
		insertEmoticons(emoticons,sentiment);
		//mongo.close();
	}
	
	public void truncateCollections(){
		database.getCollection(MongoCollection.TWEETS.getMongoName()).deleteMany(new Document());
		database.getCollection(MongoCollection.EMOJI.getMongoName()).deleteMany(new Document());
		database.getCollection(MongoCollection.EMOTICONS.getMongoName()).deleteMany(new Document());
		database.getCollection(MongoCollection.HASHTAGS.getMongoName()).deleteMany(new Document());
		
		//non ho bisogno di truncate perchè uso option REPLACE
		/*database.getCollection(MongoCollection.TWEETS_REDUCED.getMongoName()).deleteMany(new Document());
		database.getCollection(MongoCollection.EMOJI_REDUCED.getMongoName()).deleteMany(new Document());
		database.getCollection(MongoCollection.EMOTICONS_REDUCED.getMongoName()).deleteMany(new Document());
		database.getCollection(MongoCollection.HASHTAGS_REDUCED.getMongoName()).deleteMany(new Document());*/
	}
	
	public void executeMapReduce(MongoCollection inputCollection, MongoCollection outputCollection){
		System.out.println("MONGODB: Executing mapreduce for collection "+ inputCollection.getMongoName());
		/*DB db = new Mongo(host, port).getDB(database.getName());
		MapReduceCommand command = new MapReduceCommand(db.getCollection(inputCollection.getMongoName()), Constants.mapFunction, Constants.reduceFunction,outputCollection.getMongoName(), MapReduceCommand.OutputType.REPLACE, null);
		db.getCollection(inputCollection.getMongoName()).mapReduce(command);*/
		
		Bson command = new Document()
				.append("mapreduce", inputCollection.getMongoName())
				.append("map", Constants.mapFunction)
				.append("reduce", Constants.reduceFunction)
				.append("out", new Document()
						.append("merge", outputCollection.getMongoName()))
						/*.append("sharded", false)
						.append("nonAtomic", false)*/;
		database.runCommand(command);
	}
	
	public List<Emoji> getMostFreqEmoji(){
		List<Emoji> result = new ArrayList<>();
		FindIterable<Document> results = database.getCollection(MongoCollection.EMOJI_REDUCED.getMongoName()).find().limit( MainTweetProcessor.EMOJI_LIMIT ).sort(Sorts.descending("value"));
        for (Document doc : results) {
        	Emoji item = new Emoji();
        	item.setWord(doc.get("_id",Document.class).getString("word"));
        	item.setFrequency(doc.getDouble("value").intValue());
        	result.add(item);
        }
        return result;
	}
	
	private boolean existInMinus(Document doc,FindIterable<Document> resultsMinus){
		for (Document docMinus : resultsMinus) {
			if(doc.get("_id",Document.class).getString("word").equals(docMinus.get("_id",Document.class).getString("word"))){
				return true;
			}
		}
		return false;
	}
	
	public List<Emoticon> getMostFreqEmoticon(SentimentEnum sentiment){
		List<Emoticon> result = new ArrayList<>();
		BasicDBObject query = new BasicDBObject();
		query.put("_id.sentiment", sentiment.name());
		FindIterable<Document> results = database.getCollection(MongoCollection.EMOTICONS_REDUCED.getMongoName()).find(query).limit( MainTweetProcessor.EMOTICON_LIMIT ).sort(Sorts.descending("value"));
        
		BasicDBObject queryMinus = new BasicDBObject();
		queryMinus.put("value", new BasicDBObject("$gt", Constants.FREQUENCY_TRESHOLD));
		queryMinus.put("_id.sentiment", new BasicDBObject("$ne", sentiment.name()));
		FindIterable<Document> resultsMinus = database.getCollection(MongoCollection.EMOTICONS_REDUCED.getMongoName()).find(queryMinus).limit( MainTweetProcessor.EMOTICON_LIMIT ).sort(Sorts.descending("value"));
		
		for (Document doc : results) {
			
			if(existInMinus(doc,resultsMinus)){
				continue;
			}
			
        	Emoticon item = new Emoticon();
        	item.setWord(doc.get("_id",Document.class).getString("word"));
        	item.setFrequency(doc.getDouble("value").intValue());
        	result.add(item);
        }
        return result;
	}
	
	public List<HashTag> getMostFreqHashTag(SentimentEnum sentiment){
		List<HashTag> result = new ArrayList<>();
		BasicDBObject query = new BasicDBObject();
		query.put("_id.sentiment", sentiment.name());
		FindIterable<Document> results = database.getCollection(MongoCollection.HASHTAGS_REDUCED.getMongoName()).find(query).limit( MainTweetProcessor.HASHTAG_LIMIT ).sort(Sorts.descending("value"));
        
		BasicDBObject queryMinus = new BasicDBObject();
		queryMinus.put("value", new BasicDBObject("$gt", Constants.FREQUENCY_TRESHOLD));
		queryMinus.put("_id.sentiment", new BasicDBObject("$ne", sentiment.name()));
		FindIterable<Document> resultsMinus = database.getCollection(MongoCollection.HASHTAGS_REDUCED.getMongoName()).find(queryMinus).limit( MainTweetProcessor.HASHTAG_LIMIT ).sort(Sorts.descending("value"));
		
		for (Document doc : results) {
			
			if(existInMinus(doc,resultsMinus)){
				continue;
			}
			
        	HashTag item = new HashTag();
        	item.setWord(doc.get("_id",Document.class).getString("word"));
        	item.setFrequency(doc.getDouble("value").intValue());
        	result.add(item);
        }
        return result;
	}
	
	public List<Tweet> getMostFreqTweet(SentimentEnum sentiment){
		List<Tweet> result = new ArrayList<>();
		BasicDBObject query = new BasicDBObject();
		query.put("_id.sentiment", sentiment.name());
		FindIterable<Document> results = database.getCollection(MongoCollection.TWEETS_REDUCED.getMongoName()).find(query).limit( MainTweetProcessor.TWEET_LIMIT ).sort(Sorts.descending("value"));
        
		BasicDBObject queryMinus = new BasicDBObject();
		queryMinus.put("value", new BasicDBObject("$gt", Constants.FREQUENCY_TRESHOLD));
		queryMinus.put("_id.sentiment", new BasicDBObject("$ne", sentiment.name()));
		FindIterable<Document> resultsMinus = database.getCollection(MongoCollection.TWEETS_REDUCED.getMongoName()).find(queryMinus).limit( MainTweetProcessor.TWEET_LIMIT ).sort(Sorts.descending("value"));
		
		for (Document doc : results) {
			
			if(existInMinus(doc,resultsMinus)){
				continue;
			}
			
        	Tweet item = new Tweet();
        	item.setWord(doc.get("_id",Document.class).getString("word"));
        	item.setFrequency(doc.getDouble("value").intValue());
        	result.add(item);
        }
        return result;
	}
	
	private void tryToRestoreConnection() {
		System.out.println("Trying to restore mongo connection...");
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// Creating Credentials
		MongoCredential credential = MongoCredential.createCredential(user, dbName, password.toCharArray());

		mongo = MongoClients.create(MongoClientSettings.builder()
				.applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress(host, port))))
				.credential(credential).build());

		// Accessing the database
		database = mongo.getDatabase(dbName);
		System.out.println("MONGODB: Connection to database correctly restored");
	}
	
	private void insertLemmas(List<String> lemmas,SentimentEnum sentiment){
		if(lemmas.isEmpty()){
			return;
		}
		List<Document> lemmasDocuments = createDocumentList(lemmas,sentiment);
		try {
			database.getCollection(MongoCollection.TWEETS.getMongoName()).insertMany(lemmasDocuments);
		} catch (Exception e) {
			e.printStackTrace();
			tryToRestoreConnection();
			database.getCollection(MongoCollection.TWEETS.getMongoName()).insertMany(lemmasDocuments);
		}
	}
	
	private void insertHashTags(List<String> hashtags,SentimentEnum sentiment){
		if(hashtags.isEmpty()){
			return;
		}
		List<Document> hashtagsDocuments = createDocumentList(hashtags,sentiment);
		try {
			database.getCollection(MongoCollection.HASHTAGS.getMongoName()).insertMany(hashtagsDocuments);
		} catch (Exception e) {
			e.printStackTrace();
			tryToRestoreConnection();
			database.getCollection(MongoCollection.HASHTAGS.getMongoName()).insertMany(hashtagsDocuments);
		}
	}
	
	private void insertEmoji(List<String> emoji,SentimentEnum sentiment){
		if(emoji.isEmpty()){
			return;
		}
		List<Document> emojiDocuments = createDocumentList(emoji,sentiment);
		try {
			database.getCollection(MongoCollection.EMOJI.getMongoName()).insertMany(emojiDocuments);
		} catch (Exception e) {
			e.printStackTrace();
			tryToRestoreConnection();
			database.getCollection(MongoCollection.EMOJI.getMongoName()).insertMany(emojiDocuments);
		}
	}
	
	private void insertEmoticons(List<String> emoticons,SentimentEnum sentiment){
		if(emoticons.isEmpty()){
			return;
		}
		List<Document> emoticonsDocuments = createDocumentList(emoticons,sentiment);
		try {
			database.getCollection(MongoCollection.EMOTICONS.getMongoName()).insertMany(emoticonsDocuments);
		} catch (Exception e) {
			e.printStackTrace();
			tryToRestoreConnection();
			database.getCollection(MongoCollection.EMOTICONS.getMongoName()).insertMany(emoticonsDocuments);
		}
	}
	
	
	private List<Document> createDocumentList(List<String> words, SentimentEnum sentiment){
		List<Document> result = new ArrayList<>();
		
		for(String word : words){
			ObjectId objId = new ObjectId();
			Document wordDoc = new Document("objectid",objId.toString()).append("word",word).append("sentiment", sentiment.name());
			result.add(wordDoc);
		}
		
		return result;
	}
	
	private void checkCollectionsAndCreate(){
		boolean tweetExist = false;
		boolean hashtagExist = false;
		boolean emojiExist = false;
		boolean emoticonsExist = false;
		
		for (String name : database.listCollectionNames()) {
			if(name.equals(MongoCollection.TWEETS.getMongoName())){
				tweetExist = true;
			}
			else if(name.equals(MongoCollection.HASHTAGS.getMongoName())){
				hashtagExist = true;
			}
			else if(name.equals(MongoCollection.EMOTICONS.getMongoName())){
				emoticonsExist = true;
			}
			else if(name.equals(MongoCollection.EMOJI.getMongoName())){
				emojiExist = true;
			}
		}
		
		if(!tweetExist){
			System.out.println("MONGODB: tweets collection not exist, creating...");
			ValidationOptions collOptions = new ValidationOptions().validator(
			        Filters.and(Filters.exists("objectid"), Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.toStringSet())));
			database.createCollection(MongoCollection.TWEETS.getMongoName(),
			        new CreateCollectionOptions().validationOptions(collOptions));
		}
		
		if(!hashtagExist){
			System.out.println("MONGODB: hashtags collection not exist, creating...");
			ValidationOptions collOptions = new ValidationOptions().validator(
			        Filters.and(Filters.exists("objectid"), Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.toStringSet())));
			database.createCollection(MongoCollection.HASHTAGS.getMongoName(),
			        new CreateCollectionOptions().validationOptions(collOptions));
		}
		
		if(!emoticonsExist){
			System.out.println("MONGODB: emoticons collection not exist, creating...");
			ValidationOptions collOptions = new ValidationOptions().validator(
			        Filters.and(Filters.exists("objectid"), Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.toStringSet())));
			database.createCollection(MongoCollection.EMOTICONS.getMongoName(),
			        new CreateCollectionOptions().validationOptions(collOptions));
		}
		
		if(!emojiExist){
			System.out.println("MONGODB: emoji collection not exist, creating...");
			ValidationOptions collOptions = new ValidationOptions().validator(
			        Filters.and(Filters.exists("objectid"), Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.toStringSet())));
			database.createCollection(MongoCollection.EMOJI.getMongoName(),
			        new CreateCollectionOptions().validationOptions(collOptions));
		}
	}
	
	
	public static void intializeCollections(String user, String dbName,String password,String host,int port){
		
		// Creating Credentials
		MongoCredential credential = MongoCredential.createCredential(user, dbName, password.toCharArray());

		MongoClient mongo = MongoClients.create(MongoClientSettings.builder()
				.applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress(host, port))))
				.credential(credential).build());
		
		MongoDatabase database = mongo.getDatabase(dbName);

		boolean tweetExist = false;
		boolean hashtagExist = false;
		boolean emojiExist = false;
		boolean emoticonsExist = false;
		
		for (String name : database.listCollectionNames()) {
			if(name.equals(MongoCollection.TWEETS.getMongoName())){
				tweetExist = true;
			}
			else if(name.equals(MongoCollection.HASHTAGS.getMongoName())){
				hashtagExist = true;
			}
			else if(name.equals(MongoCollection.EMOTICONS.getMongoName())){
				emoticonsExist = true;
			}
			else if(name.equals(MongoCollection.EMOJI.getMongoName())){
				emojiExist = true;
			}
		}
		
		if(!tweetExist){
			System.out.println("MONGODB: tweets collection not exist, creating...");
			ValidationOptions collOptions = new ValidationOptions().validator(
			        Filters.and(Filters.exists("objectid"), Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.toStringSet())));
			database.createCollection(MongoCollection.TWEETS.getMongoName(),
			        new CreateCollectionOptions().validationOptions(collOptions));
		}
		
		if(!hashtagExist){
			System.out.println("MONGODB: hashtags collection not exist, creating...");
			ValidationOptions collOptions = new ValidationOptions().validator(
			        Filters.and(Filters.exists("objectid"), Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.toStringSet())));
			database.createCollection(MongoCollection.HASHTAGS.getMongoName(),
			        new CreateCollectionOptions().validationOptions(collOptions));
		}
		
		if(!emoticonsExist){
			System.out.println("MONGODB: emoticons collection not exist, creating...");
			ValidationOptions collOptions = new ValidationOptions().validator(
			        Filters.and(Filters.exists("objectid"), Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.toStringSet())));
			database.createCollection(MongoCollection.EMOTICONS.getMongoName(),
			        new CreateCollectionOptions().validationOptions(collOptions));
		}
		
		if(!emojiExist){
			System.out.println("MONGODB: emoji collection not exist, creating...");
			ValidationOptions collOptions = new ValidationOptions().validator(
			        Filters.and(Filters.exists("objectid"), Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.toStringSet())));
			database.createCollection(MongoCollection.EMOJI.getMongoName(),
			        new CreateCollectionOptions().validationOptions(collOptions));
		}
		
		mongo.close();
	}
	
	public void closeConnection(){
		if(mongo != null){
			mongo.close();
		}
	}
	

}
