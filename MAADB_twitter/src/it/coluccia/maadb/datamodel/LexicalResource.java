package it.coluccia.maadb.datamodel;

public class LexicalResource {
	
	private Integer id;
	private String word;
	private Integer emosnFreq;
	private Integer nrcFreq;
	private Integer sentisenseFreq;
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
	public Integer getEmosnFreq() {
		return emosnFreq;
	}
	public void setEmosnFreq(Integer emosnFreq) {
		this.emosnFreq = emosnFreq;
	}
	public Integer getNrcFreq() {
		return nrcFreq;
	}
	public void setNrcFreq(Integer nrcFreq) {
		this.nrcFreq = nrcFreq;
	}
	public Integer getSentisenseFreq() {
		return sentisenseFreq;
	}
	public void setSentisenseFreq(Integer sentisenseFreq) {
		this.sentisenseFreq = sentisenseFreq;
	}
	public Integer getSentimentIdFk() {
		return sentimentIdFk;
	}
	public void setSentimentIdFk(Integer sentimentIdFk) {
		this.sentimentIdFk = sentimentIdFk;
	}
	
	@Override
	public String toString() {
		return "LexicalResource [id=" + id + ", word=" + word + ", emosnFreq=" + emosnFreq + ", nrcFreq=" + nrcFreq
				+ ", sentisenseFreq=" + sentisenseFreq + ", sentimentIdFk=" + sentimentIdFk + "]";
	}
	
	
	

}
