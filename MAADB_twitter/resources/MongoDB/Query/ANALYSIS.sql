/*TWEET ANGER words trovati che non sono presenti tra i lexical resource = 190 su 200*/
SELECT dati1.word,
  t1.frequency
FROM
  (SELECT word
  FROM
    (SELECT T1.WORD,
      T1.FREQUENCY
    FROM TWEET T1
    WHERE SENTIMENT_FK= 1
    AND T1.WORD NOT  IN
      (SELECT T2.WORD
      FROM TWEET T2
      WHERE T2.SENTIMENT_FK != T1.SENTIMENT_FK
      AND T2.FREQUENCY       >80
      )
    ORDER BY FREQUENCY DESC
    FETCH FIRST 200 ROWS ONLY
    )
  MINUS
  SELECT word FROM lexicalresource WHERE sentiment_fk = 1
  ) dati1,
  tweet t1
WHERE upper(dati1.word)    = upper(t1.word)
AND t1.sentiment_fk =1;

/*TWEET ANTICIPATION words trovati che non sono presenti tra i lexical resource = 200 su 200*/
SELECT dati1.word,
  t1.frequency
FROM
  (SELECT word
  FROM
    (SELECT T1.WORD,
      T1.FREQUENCY
    FROM TWEET T1
    WHERE SENTIMENT_FK= 2
    AND T1.WORD NOT  IN
      (SELECT T2.WORD
      FROM TWEET T2
      WHERE T2.SENTIMENT_FK != T1.SENTIMENT_FK
      AND T2.FREQUENCY       >80
      )
    ORDER BY FREQUENCY DESC
    FETCH FIRST 200 ROWS ONLY
    )
  MINUS
  SELECT word FROM lexicalresource WHERE sentiment_fk = 2
  ) dati1,
  tweet t1
WHERE upper(dati1.word)    = upper(t1.word)
AND t1.sentiment_fk =2;

/*TWEET DISGUST words trovati che non sono presenti tra i lexical resource = 200 su 200*/
SELECT dati1.word,
  t1.frequency
FROM
  (SELECT word
  FROM
    (SELECT T1.WORD,
      T1.FREQUENCY
    FROM TWEET T1
    WHERE SENTIMENT_FK= 3
    AND T1.WORD NOT  IN
      (SELECT T2.WORD
      FROM TWEET T2
      WHERE T2.SENTIMENT_FK != T1.SENTIMENT_FK
      AND T2.FREQUENCY       >80
      )
    ORDER BY FREQUENCY DESC
    FETCH FIRST 200 ROWS ONLY
    )
  MINUS
  SELECT word FROM lexicalresource WHERE sentiment_fk = 3
  ) dati1,
  tweet t1
WHERE upper(dati1.word)    = upper(t1.word)
AND t1.sentiment_fk =3;
