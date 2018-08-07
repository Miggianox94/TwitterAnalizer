package it.coluccia.maadb.utils;

import it.coluccia.maadb.dataaccess.MongoDBDAO;

public class MongoCollectionsInitializer {

	private static final String USER = "twitter_user";
	private static final String DBNAME = "maadb_twitter";
	private static final String PASSWORD = "twitter_password";
	private static final String HOST = "178.128.32.182";
	private static final int PORT = 27017;
	
	public static void main(String[] args){
		MongoDBDAO.intializeCollections(USER, DBNAME, PASSWORD, HOST, PORT);
	}
}