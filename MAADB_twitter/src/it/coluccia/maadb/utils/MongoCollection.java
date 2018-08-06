package it.coluccia.maadb.utils;

import java.util.HashSet;
import java.util.Set;

public enum MongoCollection {
	TWEETS("tweets"),
    HASHTAGS("hashtags"),
    EMOTICONS("emoticons"),
    EMOJI("emoji");
	
	private String mongoName;
	
	private MongoCollection(String mongoName){
		this.mongoName = mongoName;
	}

	public String getMongoName() {
		return mongoName;
	}

	public void setMongoName(String mongoName) {
		this.mongoName = mongoName;
	}



	
}
