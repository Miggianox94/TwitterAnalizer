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
import it.coluccia.maadb.datamodel.LexicalResource;
import it.coluccia.maadb.utils.SentimentEnum;

public class MainLexResUploader {

	/**
	 * args[0] = jdbcurl. jdbc:oracle:thin:@localhost:1521:xe args[1] =
	 * username. args[2] = password
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String rootDir = System.getProperty("user.dir");

		if (args.length != 3) {
			System.out.println("!!!! YOU MUST PASS 3 PARAMETERS --> ABORT !!!!");
			System.exit(1);
		}

		String jdbcUrl = args[0];
		String username = args[1];
		String password = args[2];

		System.out.println("############## LEXICAL RESOURCE UPLOAD STARTED ##############");

		Connection conn = null;

		try {

			conn = createOracleDBConnection(jdbcUrl, username, password);

			System.out.println("############## CONNECTION TO ORACLE DB CREATED ##############");

			uploadLexicalResource(SentimentEnum.ANGER, rootDir + "/resources/LexicalResources/Anger/", conn);

			System.out.println("############## ANGER RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.ANTICIPATION, rootDir + "/resources/LexicalResources/Anger/", conn);

			System.out.println("############## ANTICIPATION RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.DISGUST, rootDir + "/resources/LexicalResources/Anger/", conn);

			System.out.println("############## DISGUST RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.FEAR, rootDir + "/resources/LexicalResources/Anger/", conn);

			System.out.println("############## FEAR RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.JOY, rootDir + "/resources/LexicalResources/Anger/", conn);

			System.out.println("############## JOY RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.SADNESS, rootDir + "/resources/LexicalResources/Anger/", conn);

			System.out.println("############## SADNESS RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.SURPRISE, rootDir + "/resources/LexicalResources/Anger/", conn);

			System.out.println("############## SURPRISE RESOURCE UPLOADED ##############");

			uploadLexicalResource(SentimentEnum.TRUST, rootDir + "/resources/LexicalResources/Anger/", conn);

			System.out.println("############## TRUST RESOURCE UPLOADED ##############");

			commitConnection(conn);

			System.out.println("############## CONNECTION TO ORACLE DB COMMITTED ##############");

		} catch (Exception e) {
			System.out.println("!!!! ERROR OCCURRED --> ABORT !!!!");

			try {
				rollbackConnection(conn);
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

	private static void rollbackConnection(Connection conn) throws SQLException {
		if (conn != null && !conn.isClosed()) {
			conn.rollback();
			conn.close();
		}
	}

	private static Connection createOracleDBConnection(String jdbcUrl, String username, String password)
			throws SQLException {
		Connection conn = null;

		conn = DriverManager.getConnection(jdbcUrl, username, password);
		conn.setAutoCommit(false);

		return conn;
	}

	private static void uploadLexicalResource(SentimentEnum sentiment, String fileDir, Connection conn) throws IOException, SQLException {
		System.out.println("------ Uploading resources for sentiment " + sentiment);
		File sentimentFolder = new File(fileDir);
		if (sentimentFolder.exists() && sentimentFolder.isDirectory()) {
			for (File file : sentimentFolder.listFiles()) {
				if (file.getName().startsWith("EmoSN")) {
					System.out.println("------ Uploading lexical resource of model EmoSN ");
					String fileString = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
					String[] words = fileString.split("\r\n");
					for (String word : words) {
						if (!StringUtils.isNullOrEmpty(word)) {
							System.out.println("------ Uploading lexical resource " + word);
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
				} else if (file.getName().startsWith("NRC")) {

				} else if (file.getName().startsWith("sentisense")) {

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
			preparedStatement.setInt(4, updatedRecord.getSentimentIdFk());
			
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

		String selectSQL = "INSERT INTO LEXICALRESOURCE VALUES WORD = ?, SENTIMENT_FK = ?, EMOSN_FREQ = ?,NRC_FREQ = ?,SENTISENSE_FREQ = ? ";

		try {
			preparedStatement = conn.prepareStatement(selectSQL);
			preparedStatement.setString(1, word);
			preparedStatement.setInt(2, sentiment.getTableId());
			if ("EmoSN".equals(modelName)) {
				preparedStatement.setInt(3, 1);
			} else if ("NRC".equals(modelName)) {
				preparedStatement.setInt(4, 1);
			}
			else if("sentisense".equals(modelName)){
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

			rs.next();

			result.setId(rs.getInt("ID"));
			result.setEmosnFreq(rs.getInt("EMOSN_FREQ"));
			result.setNrcFreq(rs.getInt("NRC_FREQ"));
			result.setSentimentIdFk(rs.getInt("SENTIMENT_FK"));
			result.setSentisenseFreq(rs.getInt("SENTISENSE_FREQ"));
			result.setWord(rs.getString("WORD"));

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
