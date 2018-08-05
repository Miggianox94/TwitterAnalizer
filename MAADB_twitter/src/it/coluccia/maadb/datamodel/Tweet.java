package it.coluccia.maadb.datamodel;

public class Tweet {
	
	private Integer id;
	private String word;
	private Integer frequency;
	private Integer sentimentIdFk;
	
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public Integer getFrequency() {
		return frequency;
	}
	public void setFrequency(Integer frequency) {
		this.frequency = frequency;
	}
	public Integer getSentimentIdFk() {
		return sentimentIdFk;
	}
	public void setSentimentIdFk(Integer sentimentIdFk) {
		this.sentimentIdFk = sentimentIdFk;
	}
	
	@Override
	public String toString() {
		return "Tweet [id=" + id + ", word=" + word + ", frequency=" + frequency + ", sentimentIdFk=" + sentimentIdFk
				+ "]";
	}
	
	
	
	
	

}
