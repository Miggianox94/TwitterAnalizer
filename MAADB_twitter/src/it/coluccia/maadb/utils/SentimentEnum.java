package it.coluccia.maadb.utils;

import java.util.HashSet;
import java.util.Set;

public enum SentimentEnum {
    ANGER(1,"dataset_dt_anger_60k.txt"),
    ANTICIPATION(2,"dataset_dt_anticipation_60k.txt"),
    DISGUST(3,"dataset_dt_disgust_60k.txt"),
    FEAR(4,"dataset_dt_fear_60k.txt"),
    JOY(5,"dataset_dt_joy_60k.txt"),
    SADNESS(6,"dataset_dt_sadness_60k.txt"),
    SURPRISE(7,"dataset_dt_surprise_60k.txt"),
    TRUST(8,"dataset_dt_trust_60k.txt");
	
	private int tableId;
	private String fileName;
	
	private SentimentEnum(int tableId, String fileName){
		this.tableId = tableId;
		this.fileName = fileName;
	}

	public int getTableId() {
		return tableId;
	}

	public void setTableId(int tableId) {
		this.tableId = tableId;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	public static Set<String> toStringSet(){
		Set<String> set = new HashSet<>();
		set.add(ANGER.name());
		set.add(ANTICIPATION.name());
		set.add(DISGUST.name());
		set.add(FEAR.name());
		set.add(JOY.name());
		set.add(SADNESS.name());
		set.add(SURPRISE.name());
		set.add(TRUST.name());
		
		return set;
	}
	
	public static Set<Integer> getIds(){
		Set<Integer> result = new HashSet<>();
		result.add(ANGER.getTableId());
		result.add(ANTICIPATION.getTableId());
		result.add(DISGUST.getTableId());
		result.add(FEAR.getTableId());
		result.add(JOY.getTableId());
		result.add(SADNESS.getTableId());
		result.add(SURPRISE.getTableId());
		result.add(TRUST.getTableId());
		return result;
	}
	
	public static SentimentEnum getSentimentFromId(Integer id){
		for(SentimentEnum sentiment : SentimentEnum.values()){
			if(sentiment.getTableId() == id){
				return sentiment;
			}
		}
		return null;
	}
	
	
	
	
}
