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