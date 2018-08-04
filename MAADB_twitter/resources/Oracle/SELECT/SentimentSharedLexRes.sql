/*Quali sono le risorse lessicali che sono condivise tra più sentimenti?*/
select distinct l1.word
from lexicalresource l1, lexicalresource l2
where l1.word = l2.word and l1.id != l2.id;