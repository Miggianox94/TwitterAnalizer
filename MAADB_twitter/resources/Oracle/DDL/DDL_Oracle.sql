CREATE TABLE "Sentiment" (
	"ID" NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
	"NAME" varchar,
	constraint SENTIMENT_PK PRIMARY KEY ("ID")
)

CREATE TABLE "Tweet" (
	"ID" NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
	"WORD" varchar,
	"FREQUENCY" INT,
	"SENTIMENT_FK" INT,
	constraint TWEET_PK PRIMARY KEY ("ID")
)

CREATE TABLE "HashTag" (
	"ID" NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
	"WORD" varchar,
	"FREQUENCY" INT,
	"SENTIMENT_FK" INT,
	constraint HASHTAG_PK PRIMARY KEY ("ID")
)

CREATE TABLE "Emoji" (
	"ID" NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
	"WORD" varchar,
	"FREQUENCY" INT,
	"SENTIMENT_FK" INT,
	constraint EMOJI_PK PRIMARY KEY ("ID")
)

CREATE TABLE "Emoticon" (
	"ID" NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
	"WORD" varchar,
	"FREQUENCY" INT,
	"SENTIMENT_FK" INT,
	constraint EMOTICON_PK PRIMARY KEY ("ID")
)

CREATE TABLE "LexicalResource" (
	"ID" NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
	"WORD" varchar,
	"EMOSN_FREQ" INT,
	"NRC_FREQ" INT,
	"SENTISENSE_FREQ" INT,
	"SENTIMENT_FK" INT,
	constraint LEXICALRESOURCE_PK PRIMARY KEY ("ID")
)


ALTER TABLE "Tweet" ADD CONSTRAINT "Tweet_fk0" FOREIGN KEY ("SENTIMENT_FK") REFERENCES Sentiment("ID");

ALTER TABLE "HashTag" ADD CONSTRAINT "HashTag_fk0" FOREIGN KEY ("SENTIMENT_FK") REFERENCES Sentiment("ID");

ALTER TABLE "Emoji" ADD CONSTRAINT "Emoji_fk0" FOREIGN KEY ("SENTIMENT_FK") REFERENCES Sentiment("ID");

ALTER TABLE "Emoticon" ADD CONSTRAINT "Emoticon_fk0" FOREIGN KEY ("SENTIMENT_FK") REFERENCES Sentiment("ID");

ALTER TABLE "LexicalResource" ADD CONSTRAINT "LexicalResource_fk0" FOREIGN KEY ("SENTIMENT_FK") REFERENCES Sentiment("ID");

ALTER TABLE "LexicalResource" ADD CONSTRAINT "LexicalResource_UC" UNIQUE ("WORD","SENTIMENT_FK");
