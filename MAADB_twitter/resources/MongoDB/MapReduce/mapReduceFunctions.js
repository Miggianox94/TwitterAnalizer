/*------------------------*/

db.system.js.save(
   {
     _id: "mapTwitterFunction",
     value : function() {  emit({ 'sentiment ': this.sentiment , 'word ': this.word }, 1); }
   }
)

/*------------------------*/

db.system.js.save(
   {
     _id : "reduceTwitterFunction" ,
     value : function(key, values) {return Array.sum(values);}
   }
);

/*------------------------*/


function(key, values) {
    reducedVal = {
        'EmoSN': 0,
        'NRC': 0,
        'sentisense': 0
    };
    for (var idx = 0; idx < values.length; idx++) {
        reducedVal.EmoSN += values[idx].EmoSN;
        reducedVal.NRC += values[idx].NRC;
        reducedVal.sentisense += values[idx].sentisense;
    }

    return reducedVal;
};