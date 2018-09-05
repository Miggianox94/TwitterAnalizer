/*TWEET words trovati che non sono presenti tra i lexical resource = 197 su 200*/
select dati1.word,t1.frequency from(
select word from(
select word
from tweet
where sentiment_fk = 1
order by frequency desc
)where rownum < 200
minus
select word
from lexicalresource
where sentiment_fk = 1) dati1, tweet t1
where dati1.word = t1.word and t1.sentiment_fk =1;