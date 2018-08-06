package it.coluccia.maadb.dataaccess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ValidationOptions;

import it.coluccia.maadb.utils.MongoCollection;
import it.coluccia.maadb.utils.SentimentEnum;

public class MongoDBDAO {

	MongoClient mongo;
	MongoDatabase database;

	public MongoDBDAO(String host, int port, String user, String password, String dbName) {
		
		// Creating Credentials
		MongoCredential credential = MongoCredential.createCredential(user, dbName, password.toCharArray());
		
		mongo = MongoClients.create(
		        MongoClientSettings.builder()
		                .applyToClusterSettings(builder -> 
		                        builder.hosts(Arrays.asList(new ServerAddress(host, port))))
		                .credential(credential)
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
		mongo.close();
	}
	
	public void truncateCollections(){
		database.getCollection(MongoCollection.TWEETS.name()).drop();
		database.getCollection(MongoCollection.EMOJI.name()).drop();
		database.getCollection(MongoCollection.EMOTICONS.name()).drop();
		database.getCollection(MongoCollection.HASHTAGS.name()).drop();
	}
	
	private void insertLemmas(List<String> lemmas,SentimentEnum sentiment){
		if(lemmas.isEmpty()){
			return;
		}
		List<Document> lemmasDocuments = createDocumentList(lemmas,sentiment);
		database.getCollection(MongoCollection.TWEETS.name()).insertMany(lemmasDocuments);
	}
	
	private void insertHashTags(List<String> hashtags,SentimentEnum sentiment){
		if(hashtags.isEmpty()){
			return;
		}
		List<Document> hashtagsDocuments = createDocumentList(hashtags,sentiment);
		database.getCollection(MongoCollection.TWEETS.name()).insertMany(hashtagsDocuments);
	}
	
	private void insertEmoji(List<String> emoji,SentimentEnum sentiment){
		if(emoji.isEmpty()){
			return;
		}
		List<Document> emojiDocuments = createDocumentList(emoji,sentiment);
		database.getCollection(MongoCollection.TWEETS.name()).insertMany(emojiDocuments);
	}
	
	private void insertEmoticons(List<String> emoticons,SentimentEnum sentiment){
		if(emoticons.isEmpty()){
			return;
		}
		List<Document> emoticonsDocuments = createDocumentList(emoticons,sentiment);
		database.getCollection(MongoCollection.TWEETS.name()).insertMany(emoticonsDocuments);
	}
	
	
	private List<Document> createDocumentList(List<String> words, SentimentEnum sentiment){
		List<Document> result = new ArrayList<>();
		
		for(String word : words){
			Document wordDoc = new Document("word",word).append("sentiment", sentiment);
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
			
			if(!tweetExist){
				System.out.println("MONGODB: tweets collection not exist, creating...");
				ValidationOptions collOptions = new ValidationOptions().validator(
				        Filters.and(Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.values())));
				database.createCollection(MongoCollection.TWEETS.getMongoName(),
				        new CreateCollectionOptions().validationOptions(collOptions));
			}
			
			if(!hashtagExist){
				System.out.println("MONGODB: hashtags collection not exist, creating...");
				ValidationOptions collOptions = new ValidationOptions().validator(
				        Filters.and(Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.values())));
				database.createCollection(MongoCollection.HASHTAGS.getMongoName(),
				        new CreateCollectionOptions().validationOptions(collOptions));
			}
			
			if(!emoticonsExist){
				System.out.println("MONGODB: emoticons collection not exist, creating...");
				ValidationOptions collOptions = new ValidationOptions().validator(
				        Filters.and(Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.values())));
				database.createCollection(MongoCollection.EMOTICONS.getMongoName(),
				        new CreateCollectionOptions().validationOptions(collOptions));
			}
			
			if(!emojiExist){
				System.out.println("MONGODB: emoji collection not exist, creating...");
				ValidationOptions collOptions = new ValidationOptions().validator(
				        Filters.and(Filters.exists("word"), Filters.exists("sentiment"),Filters.in("sentiment",SentimentEnum.values())));
				database.createCollection(MongoCollection.EMOJI.getMongoName(),
				        new CreateCollectionOptions().validationOptions(collOptions));
			}
			
		}
	}
	

}
