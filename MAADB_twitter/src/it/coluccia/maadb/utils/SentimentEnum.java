package it.coluccia.maadb.utils;

public enum SentimentEnum {
    ANGER(1),
    ANTICIPATION(2),
    DISGUST(3),
    FEAR(4),
    JOY(5),
    SADNESS(6),
    SURPRISE(7),
    TRUST(8);
	
	private int tableId;
	
	private SentimentEnum(int tableId){
		this.tableId = tableId;
	}

	public int getTableId() {
		return tableId;
	}

	public void setTableId(int tableId) {
		this.tableId = tableId;
	}
	
	
}
