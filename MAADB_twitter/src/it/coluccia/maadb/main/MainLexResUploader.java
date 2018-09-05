package it.coluccia.maadb.main;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;

import edu.stanford.nlp.util.StringUtils;
import it.coluccia.maadb.dataaccess.MongoDBDAO;
import it.coluccia.maadb.datamodel.LexicalResource;
import it.coluccia.maadb.utils.SentimentEnum;

public class MainLexResUploader {

	/**
	 * args[0] = jdbcurl. jdbc:oracle:thin:@localhost:1521:SID args[1] =
	 * username. args[2] = password
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String rootDir = System.getProperty("user.dir");

		if (args.length != 9) {
			System.out.println("!!!! YOU MUST PASS 9 PARAMETERS --> ABORT !!!!");
			System.exit(1);
		}

		String jdbcUrl = args[0];
		String username = args[1];
		String password = args[2];
		
		String mongoHost = args[3];
		String mongoUser = args[4];
		String mongoPassword = args[5];
		String mongoDbName = args[6];
		int mongoPort = Integer.parseInt(args[7]);
		
		int modality = Integer.parseInt(args[8]); //1=Oracle; 2=Mongo;

		System.out.println("############## LEXICAL RESOURCE UPLOAD STARTED ##############");

		Connection conn = null;
		MongoDBDAO mongoDbDao = null;

		try {

			if (modality == 1) {
				System.out.println("############## ORACLE MODALITY ##############");
				conn = createOracleDBConnection(jdbcUrl, username, password);
			} else if (modality == 2) {
				System.out.println("############## MONGODB MODALITY ##############");
				mongoDbDao = new MongoDBDAO(mongoHost, mongoPort, mongoUser, mongoPassword, mongoDbName);
			} else {
				throw new IllegalArgumentException("Modality not known");
			}

			System.out.println("############## CONNECTION TO ORACLE DB CREATED ##############");

			uploadLexicalResource(SentimentEnum.ANGER, rootDir + "/resources/LexicalResources/Anger/", conn, mongoDbDao,
					modality);

			System.out.println("############## ANGER RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.ANTICIPATION, rootDir + "/resources/LexicalResources/Anticipation/",
					conn, mongoDbDao, modality);

			System.out.println("############## ANTICIPATION RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.DISGUST, rootDir + "/resources/LexicalResources/Disgust/", conn,
					mongoDbDao, modality);

			System.out.println("############## DISGUST RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.FEAR, rootDir + "/resources/LexicalResources/Fear/", conn, mongoDbDao,
					modality);

			System.out.println("############## FEAR RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.JOY, rootDir + "/resources/LexicalResources/Joy/", conn, mongoDbDao,
					modality);

			System.out.println("############## JOY RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.SADNESS, rootDir + "/resources/LexicalResources/Sadness/", conn,
					mongoDbDao, modality);

			System.out.println("############## SADNESS RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.SURPRISE, rootDir + "/resources/LexicalResources/Surprise/", conn,
					mongoDbDao, modality);

			System.out.println("############## SURPRISE RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.TRUST, rootDir + "/resources/LexicalResources/Trust/", conn, mongoDbDao,
					modality);

			System.out.println("############## TRUST RESOURCE UPLOADED ##############");

			commitConnection(conn);

			System.out.println("############## CONNECTION TO DB COMMITTED ##############");
			
			if (modality == 2) {
				
				mongoDbDao.executeMapReduceLexRes();
				System.out.println("############## MAPREDUCED EXECUTED ##############");
			}

		} catch (Exception e) {
			System.out.println("!!!! ERROR OCCURRED --> ABORT !!!!");

			try {
				rollbackConnection(conn,mongoDbDao);
				System.out.println("!!!! CONNECTION TO ORACLE DB ROLLBACKED !!!!");
			} catch (SQLException e1) {
				System.out.println(
						"--!!-- ATTENTION: Connection not closed and not rollbacked due to an unexpected error. Check manually the DB situation");
			}


			e.printStackTrace();
		}

		System.out.println("############## LEXICAL RESOURCE UPLOAD COMPLETED ##############");

	}

	private static void commitConnection(Connection conn) throws SQLException {
		if (conn != null && !conn.isClosed()) {
			conn.commit();
			conn.close();
		}
	}

	private static void rollbackConnection(Connection conn, MongoDBDAO mongoDbDao) throws SQLException {
		if (conn != null && !conn.isClosed()) {
			conn.rollback();
			conn.close();
		}
		
		if(mongoDbDao != null){
			mongoDbDao.closeConnection();
		}
	}

	private static Connection createOracleDBConnection(String jdbcUrl, String username, String password)
			throws SQLException {
		Connection conn = null;

		conn = DriverManager.getConnection(jdbcUrl, username, password);
		conn.setAutoCommit(false);

		return conn;
	}

	private static void uploadLexicalResource(SentimentEnum sentiment, String fileDir, Connection conn, MongoDBDAO mongodbDao, int modality) throws IOException, SQLException {
		System.out.println("------ Uploading resources for sentiment " + sentiment);
		File sentimentFolder = new File(fileDir);
		if (sentimentFolder.exists() && sentimentFolder.isDirectory()) {
			for (File file : sentimentFolder.listFiles()) {
				if (file.getName().startsWith("EmoSN")) {
					System.out.println("------ Uploading lexical resource of model EmoSN ");
					String fileString = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
					String[] words = fileString.split("\\n");
					for (String word : words) {
						if (!StringUtils.isNullOrEmpty(word)) {
							System.out.println("------ Uploading lexical resource " + word);
							
							if(modality == 2){
								mongodbDao.insertLexicalResource(word, sentiment, "EmoSN");
							}
							else{
								LexicalResource result = checkIfExist(word, sentiment, conn);
								if (result == null) {
									System.out.println("------ It is a new word " + word);
									insertLexicalResource(word, sentiment, "EmoSN", conn);
								} else {
									System.out.println("------ Word present in datastore " + result);
									result.setEmosnFreq(result.getEmosnFreq() + 1);
									updateLexicalResource(result, conn);
								}
							}
							
						}
					}
				} else if (file.getName().startsWith("NRC")) {
					System.out.println("------ Uploading lexical resource of model NRC ");
					String fileString = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
					String[] words = fileString.split("\\n");
					for (String word : words) {
						if (!StringUtils.isNullOrEmpty(word)) {
							System.out.println("------ Uploading lexical resource " + word);
							
							if(modality == 2){
								mongodbDao.insertLexicalResource(word, sentiment, "NRC");
							}
							else{
								LexicalResource result = checkIfExist(word, sentiment, conn);
								if (result == null) {
									System.out.println("------ It is a new word " + word);
									insertLexicalResource(word, sentiment, "NRC", conn);
								} else {
									System.out.println("------ Word present in datastore " + result);
									result.setNrcFreq(result.getNrcFreq() + 1);
									updateLexicalResource(result, conn);
								}
							}
						}
					}
				} else if (file.getName().startsWith("sentisense")) {
					System.out.println("------ Uploading lexical resource of model sentisense ");
					String fileString = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
					String[] words = fileString.split("\\n");
					for (String word : words) {
						if (!StringUtils.isNullOrEmpty(word)) {
							if(modality == 2){
								mongodbDao.insertLexicalResource(word, sentiment, "sentisense");
							}
							else{
								System.out.println("------ Uploading lexical resource " + word);
								LexicalResource result = checkIfExist(word, sentiment, conn);
								if (result == null) {
									System.out.println("------ It is a new word " + word);
									insertLexicalResource(word, sentiment, "sentisense", conn);
								} else {
									System.out.println("------ Word present in datastore " + result);
									result.setSentisenseFreq(result.getSentisenseFreq() + 1);
									updateLexicalResource(result, conn);
								}
							}
						}
					}
				} else {
					throw new RuntimeException(
							"!!! Lexical resource not recognized like lexical model !!! " + file.getAbsolutePath());
				}
			}
		} else {
			throw new IllegalArgumentException("!!! WRONG FILEDIR !!! " + fileDir);
		}
	}
	
	
	private static void updateLexicalResource(LexicalResource updatedRecord, Connection conn) throws SQLException{
		PreparedStatement preparedStatement = null;

		String selectSQL = "UPDATE LEXICALRESOURCE SET EMOSN_FREQ = ?,NRC_FREQ = ?,SENTISENSE_FREQ = ? WHERE ID = ? ";

		try {
			preparedStatement = conn.prepareStatement(selectSQL);
			preparedStatement.setInt(1, updatedRecord.getEmosnFreq());
			preparedStatement.setInt(2, updatedRecord.getNrcFreq());
			preparedStatement.setInt(3, updatedRecord.getSentisenseFreq());
			preparedStatement.setInt(4, updatedRecord.getId());
			
			preparedStatement.executeUpdate();

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					System.out.println(
							"--!!-- ATTENTION: preparedStatement not closed due to an unexpected error. Workflow can continue ");
					e.printStackTrace();
				}
			}

		}
	}

	private static void insertLexicalResource(String word, SentimentEnum sentiment, String modelName, Connection conn) throws SQLException {
		PreparedStatement preparedStatement = null;

		String selectSQL = "INSERT INTO LEXICALRESOURCE(WORD,SENTIMENT_FK,EMOSN_FREQ,NRC_FREQ,SENTISENSE_FREQ) VALUES (?,?,?,?,?) ";

		try {
			preparedStatement = conn.prepareStatement(selectSQL);
			preparedStatement.setString(1, word);
			preparedStatement.setInt(2, sentiment.getTableId());
			if ("EmoSN".equals(modelName)) {
				preparedStatement.setInt(3, 1);
				preparedStatement.setInt(4, 0);
				preparedStatement.setInt(5, 0);
			} else if ("NRC".equals(modelName)) {
				preparedStatement.setInt(3, 0);
				preparedStatement.setInt(4, 1);
				preparedStatement.setInt(5, 0);
			}
			else if("sentisense".equals(modelName)){
				preparedStatement.setInt(3, 0);
				preparedStatement.setInt(4, 0);
				preparedStatement.setInt(5, 1);
			}
			else{
				throw new IllegalArgumentException("!!! UNKNOWN MODEL: "+modelName);
			}

			preparedStatement.executeUpdate();

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					System.out.println(
							"--!!-- ATTENTION: preparedStatement not closed due to an unexpected error. Workflow can continue ");
					e.printStackTrace();
				}
			}

		}
	}

	private static LexicalResource checkIfExist(String word, SentimentEnum sentiment, Connection conn) throws SQLException {
		PreparedStatement preparedStatement = null;
		LexicalResource result = new LexicalResource();

		String selectSQL = "SELECT * FROM LEXICALRESOURCE WHERE WORD = ? AND SENTIMENT_FK = ?";

		try {
			preparedStatement = conn.prepareStatement(selectSQL);
			preparedStatement.setString(1, word);
			preparedStatement.setInt(2, sentiment.getTableId());

			ResultSet rs = preparedStatement.executeQuery();

			if(rs.next()){
				result.setId(rs.getInt("ID"));
				result.setEmosnFreq(rs.getInt("EMOSN_FREQ"));
				result.setNrcFreq(rs.getInt("NRC_FREQ"));
				result.setSentimentIdFk(rs.getInt("SENTIMENT_FK"));
				result.setSentisenseFreq(rs.getInt("SENTISENSE_FREQ"));
				result.setWord(rs.getString("WORD"));	
			}
			else{
				return null;
			}

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					System.out.println(
							"--!!-- ATTENTION: preparedStatement not closed due to an unexpected error. Workflow can continue ");
					e.printStackTrace();
				}
			}

		}

		return result;
	}
}
