package it.coluccia.maadb.dataaccess;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import it.coluccia.maadb.datamodel.Emoji;
import it.coluccia.maadb.datamodel.Emoticon;
import it.coluccia.maadb.datamodel.HashTag;
import it.coluccia.maadb.datamodel.Tweet;
import it.coluccia.maadb.main.MainTweetProcessor;
import it.coluccia.maadb.utils.SentimentEnum;

public class OracleDAO {

	private Connection connection;

	public OracleDAO(String jdbcUrl, String username, String password) throws SQLException {
		super();
		this.connection = createOracleDBConnection(jdbcUrl, username, password);
	}

	public void persist(List<String> lemmas, List<String> hashTags, List<String> emoji, List<String> emoticons,
			SentimentEnum sentiment) throws Exception {
		try {

			for (String lemma : lemmas) {
				Tweet existingTweet = checkIfExistLemma(lemma, sentiment.getTableId());
				if (existingTweet == null) {
					insertLemma(lemma, sentiment);
				} else {
					updateLemmaFrequency(existingTweet);
				}
			}
			for (String hashTag : hashTags) {
				HashTag existingHashTag = checkIfExistHashTag(hashTag, sentiment.getTableId());
				if (existingHashTag == null) {
					insertHashTag(hashTag, sentiment);
				} else {
					updateHashTagFrequency(existingHashTag);
				}
			}
			for (String singleEmoji : emoji) {
				Emoji existingEmoji = checkIfExistEmoji(singleEmoji, sentiment.getTableId());
				if (existingEmoji == null) {
					insertEmoji(singleEmoji, sentiment);
				} else {
					updateEmojiFrequency(existingEmoji);
				}
			}
			for (String emoticon : emoticons) {
				Emoticon existingEmoticon = checkIfExistEmoticon(emoticon, sentiment.getTableId());
				if (existingEmoticon == null) {
					insertEmoticon(emoticon, sentiment);
				} else {
					updateEmoticonFrequency(existingEmoticon);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			rollbackConnection();
			System.out.println("!!!! CONNECTION TO ORACLE DB ROLLBACKED !!!!");
			throw new Exception(e.getMessage());
		}
		finally{
			commitConnection();
		}
	}
	
	public List<Emoji> getMostFreqEmoji() throws SQLException{
		Statement statement = null;

		String selectSQL = "SELECT WORD,FREQUENCY FROM EMOJI ORDER BY FREQUENCY DESC FETCH FIRST "+MainTweetProcessor.EMOJI_LIMIT+ " ROWS ONLY";
		
		List<Emoji> result = new ArrayList<Emoji>();

		try {
			statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(selectSQL);
			while (rs.next()) {

				Emoji item = new Emoji();
				item.setWord(rs.getString("WORD"));
				item.setFrequency(rs.getInt("FREQUENCY"));
				result.add(item);
			}

		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					System.out.println(
							"--!!-- ATTENTION: preparedStatement not closed due to an unexpected error. Workflow can continue ");
					e.printStackTrace();
				}
			}

		}
		
		return result;
	}
	
	public List<Emoticon> getMostFreqEmoticon() throws SQLException{
		Statement statement = null;

		String selectSQL = "SELECT WORD,FREQUENCY FROM EMOTICON ORDER BY FREQUENCY DESC FETCH FIRST "+MainTweetProcessor.EMOTICON_LIMIT+ " ROWS ONLY";
		
		List<Emoticon> result = new ArrayList<>();

		try {
			statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(selectSQL);
			while (rs.next()) {

				Emoticon item = new Emoticon();
				item.setWord(rs.getString("WORD"));
				item.setFrequency(rs.getInt("FREQUENCY"));
				result.add(item);
			}

		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					System.out.println(
							"--!!-- ATTENTION: preparedStatement not closed due to an unexpected error. Workflow can continue ");
					e.printStackTrace();
				}
			}

		}
		
		return result;
	}
	
	public List<HashTag> getMostFreqHashTag() throws SQLException{
		Statement statement = null;

		String selectSQL = "SELECT WORD,FREQUENCY FROM HASHTAG ORDER BY FREQUENCY DESC FETCH FIRST "+MainTweetProcessor.HASHTAG_LIMIT+ " ROWS ONLY";
		
		List<HashTag> result = new ArrayList<>();

		try {
			statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(selectSQL);
			while (rs.next()) {

				HashTag item = new HashTag();
				item.setWord(rs.getString("WORD"));
				item.setFrequency(rs.getInt("FREQUENCY"));
				result.add(item);
			}

		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					System.out.println(
							"--!!-- ATTENTION: preparedStatement not closed due to an unexpected error. Workflow can continue ");
					e.printStackTrace();
				}
			}

		}
		
		return result;
	}
	
	public List<Tweet> getMostFreqTweet() throws SQLException{
		Statement statement = null;

		String selectSQL = "SELECT WORD,FREQUENCY FROM TWEET ORDER BY FREQUENCY DESC FETCH FIRST "+MainTweetProcessor.TWEET_LIMIT+ " ROWS ONLY";
		
		List<Tweet> result = new ArrayList<>();

		try {
			statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(selectSQL);
			while (rs.next()) {

				Tweet item = new Tweet();
				item.setWord(rs.getString("WORD"));
				item.setFrequency(rs.getInt("FREQUENCY"));
				result.add(item);
			}

		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					System.out.println(
							"--!!-- ATTENTION: preparedStatement not closed due to an unexpected error. Workflow can continue ");
					e.printStackTrace();
				}
			}

		}
		
		return result;
	}

	private void commitConnection() throws SQLException {
		if (connection != null && !connection.isClosed()) {
			connection.commit();
			connection.close();
			if (connection.isClosed()) {
				System.out.println("************** CONNECTION CORRECTLY CLOSED *********");
			}
		}
	}

	private void rollbackConnection() throws SQLException {
		if (connection != null && !connection.isClosed()) {
			connection.rollback();
			connection.close();
		}
	}

	private Connection createOracleDBConnection(String jdbcUrl, String username, String password) throws SQLException {
		Connection conn = null;

		conn = DriverManager.getConnection(jdbcUrl, username, password);
		conn.setAutoCommit(false);

		return conn;
	}

	private Tweet checkIfExistLemma(String lemma, Integer sentimentFk) throws SQLException {
		PreparedStatement preparedStatement = null;
		Tweet result = new Tweet();

		String selectSQL = "SELECT * FROM TWEET WHERE WORD = ? AND SENTIMENT_FK = ?";

		try {
			preparedStatement = connection.prepareStatement(selectSQL);
			preparedStatement.setString(1, lemma);
			preparedStatement.setInt(2, sentimentFk);

			ResultSet rs = preparedStatement.executeQuery();

			if (rs.next()) {
				result.setId(rs.getInt("ID"));
				result.setSentimentIdFk(rs.getInt("SENTIMENT_FK"));
				result.setFrequency(rs.getInt("FREQUENCY"));
				result.setWord(rs.getString("WORD"));
			} else {
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

	private void updateLemmaFrequency(Tweet updatedRecord) throws SQLException {
		PreparedStatement preparedStatement = null;

		String updateSQL = "UPDATE TWEET SET FREQUENCY = FREQUENCY+1 WHERE ID = ? ";

		try {

			preparedStatement = connection.prepareStatement(updateSQL);
			preparedStatement.setInt(1, updatedRecord.getId());
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

	private void insertLemma(String lemma, SentimentEnum sentiment) throws SQLException {
		PreparedStatement preparedStatement = null;

		String insertSQL = "INSERT INTO TWEET(WORD,SENTIMENT_FK,FREQUENCY) VALUES (?,?,1) ";

		try {
			preparedStatement = connection.prepareStatement(insertSQL);
			preparedStatement.setString(1, lemma);
			preparedStatement.setInt(2, sentiment.getTableId());
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

	private HashTag checkIfExistHashTag(String lemma, Integer sentimentFk) throws SQLException {
		PreparedStatement preparedStatement = null;
		HashTag result = new HashTag();

		String selectSQL = "SELECT * FROM HASHTAG WHERE WORD = ? AND SENTIMENT_FK = ?";

		try {
			preparedStatement = connection.prepareStatement(selectSQL);
			preparedStatement.setString(1, lemma);
			preparedStatement.setInt(2, sentimentFk);

			ResultSet rs = preparedStatement.executeQuery();

			if (rs.next()) {
				result.setId(rs.getInt("ID"));
				result.setSentimentIdFk(rs.getInt("SENTIMENT_FK"));
				result.setFrequency(rs.getInt("FREQUENCY"));
				result.setWord(rs.getString("WORD"));
			} else {
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

	private void updateHashTagFrequency(HashTag updatedRecord) throws SQLException {
		PreparedStatement preparedStatement = null;

		String updateSQL = "UPDATE HASHTAG SET FREQUENCY = FREQUENCY+1 WHERE ID = ? ";

		try {

			preparedStatement = connection.prepareStatement(updateSQL);
			preparedStatement.setInt(1, updatedRecord.getId());
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

	private void insertHashTag(String lemma, SentimentEnum sentiment) throws SQLException {
		PreparedStatement preparedStatement = null;

		String insertSQL = "INSERT INTO HASHTAG(WORD,SENTIMENT_FK,FREQUENCY) VALUES (?,?,1) ";

		try {
			preparedStatement = connection.prepareStatement(insertSQL);
			preparedStatement.setString(1, lemma);
			preparedStatement.setInt(2, sentiment.getTableId());
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

	private Emoticon checkIfExistEmoticon(String lemma, Integer sentimentFk) throws SQLException {
		PreparedStatement preparedStatement = null;
		Emoticon result = new Emoticon();

		String selectSQL = "SELECT * FROM EMOTICON WHERE WORD = ? AND SENTIMENT_FK = ?";

		try {
			preparedStatement = connection.prepareStatement(selectSQL);
			preparedStatement.setString(1, lemma);
			preparedStatement.setInt(2, sentimentFk);

			ResultSet rs = preparedStatement.executeQuery();

			if (rs.next()) {
				result.setId(rs.getInt("ID"));
				result.setSentimentIdFk(rs.getInt("SENTIMENT_FK"));
				result.setFrequency(rs.getInt("FREQUENCY"));
				result.setWord(rs.getString("WORD"));
			} else {
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

	private void updateEmoticonFrequency(Emoticon updatedRecord) throws SQLException {
		PreparedStatement preparedStatement = null;

		String updateSQL = "UPDATE EMOTICON SET FREQUENCY = FREQUENCY+1 WHERE ID = ? ";

		try {

			preparedStatement = connection.prepareStatement(updateSQL);
			preparedStatement.setInt(1, updatedRecord.getId());
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

	private void insertEmoticon(String lemma, SentimentEnum sentiment) throws SQLException {
		PreparedStatement preparedStatement = null;

		String insertSQL = "INSERT INTO EMOTICON(WORD,SENTIMENT_FK,FREQUENCY) VALUES (?,?,1) ";

		try {
			preparedStatement = connection.prepareStatement(insertSQL);
			preparedStatement.setString(1, lemma);
			preparedStatement.setInt(2, sentiment.getTableId());
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

	private Emoji checkIfExistEmoji(String lemma, Integer sentimentFk) throws SQLException {
		PreparedStatement preparedStatement = null;
		Emoji result = new Emoji();

		String selectSQL = "SELECT * FROM EMOJI WHERE WORD = ? AND SENTIMENT_FK = ?";

		try {
			preparedStatement = connection.prepareStatement(selectSQL);
			preparedStatement.setString(1, lemma);
			preparedStatement.setInt(2, sentimentFk);

			ResultSet rs = preparedStatement.executeQuery();

			if (rs.next()) {
				result.setId(rs.getInt("ID"));
				result.setSentimentIdFk(rs.getInt("SENTIMENT_FK"));
				result.setFrequency(rs.getInt("FREQUENCY"));
				result.setWord(rs.getString("WORD"));
			} else {
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

	private void updateEmojiFrequency(Emoji updatedRecord) throws SQLException {
		PreparedStatement preparedStatement = null;

		String updateSQL = "UPDATE EMOJI SET FREQUENCY = FREQUENCY+1 WHERE ID = ? ";

		try {

			preparedStatement = connection.prepareStatement(updateSQL);
			preparedStatement.setInt(1, updatedRecord.getId());
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

	private void insertEmoji(String lemma, SentimentEnum sentiment) throws SQLException {
		PreparedStatement preparedStatement = null;

		String insertSQL = "INSERT INTO EMOJI(WORD,SENTIMENT_FK,FREQUENCY) VALUES (?,?,1) ";

		try {
			preparedStatement = connection.prepareStatement(insertSQL);
			preparedStatement.setString(1, lemma);
			preparedStatement.setInt(2, sentiment.getTableId());
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

}
