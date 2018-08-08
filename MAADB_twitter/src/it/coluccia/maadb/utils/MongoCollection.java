package it.coluccia.maadb.utils;

public enum MongoCollection {
	TWEETS("tweets"),
    HASHTAGS("hashtags"),
    EMOTICONS("emoticons"),
    EMOJI("emoji"),
	
	TWEETS_REDUCED("tweets_reduced"),
    HASHTAGS_REDUCED("hashtags_reduced"),
    EMOTICONS_REDUCED("emoticons_reduced"),
    EMOJI_REDUCED("emoji_reduced");
    
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
