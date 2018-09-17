/*Quali sono le risorse lessicali che sono condivise tra più sentimenti?*/
select distinct l1.word
from lexicalresource l1, lexicalresource l2
where l1.word = l2.word and l1.SENTIMENT_FK != l2.SENTIMENT_FK;

/*Quali sono le risorse lessicali per lo stesso sentimento che compaiono in più file?*/
select *
from lexicalresource l1
where l1.EMOSN_FREQ+l1.NRC_FREQ+l1.SENTISENSE_FREQ > 1;