package it.coluccia.maadb.main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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

			rollbackConnection(conn);

			System.out.println("!!!! CONNECTION TO ORACLE DB ROLLBACKED !!!!");

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

	private static Connection createOracleDBConnection(String jdbcUrl, String username, String password) throws SQLException {
		Connection conn = null;

		conn = DriverManager.getConnection(jdbcUrl, username, password);
		conn.setAutoCommit(false);

		return conn;
	}
}
