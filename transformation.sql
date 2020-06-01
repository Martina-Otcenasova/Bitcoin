CREATE TABLE "commodities" AS
WITH 
b AS (SELECT "day" AS date, "price_open"::float AS bitcoin FROM "bitcoin"),

l AS (SELECT "bitcoin"."day" AS date,
      CASE
         WHEN "bitcoin"."price_open" IS NULL THEN LAG("bitcoin"."price_open"::float) IGNORE NULLS OVER (ORDER BY date)
         ELSE "bitcoin"."price_open"::float
      END AS litecoin
      FROM "bitcoin"
      LEFT JOIN "litecoin" ON "bitcoin"."day" = TO_DATE("litecoin"."day")),
      
e AS (SELECT "bitcoin"."day" AS date,
      CASE
         WHEN "ethereum"."price_open" IS NULL THEN LAG("ethereum"."price_open"::float) IGNORE NULLS OVER (ORDER BY date)
         ELSE "ethereum"."price_open"::float
      END AS ethereum
      FROM "bitcoin"
      LEFT JOIN "ethereum" ON "bitcoin"."day" = TO_DATE("ethereum"."day")),
      
r AS (SELECT "bitcoin"."day" AS date,
      CASE
         WHEN "ripple"."price_open" IS NULL THEN LAG("ripple"."price_open"::float) IGNORE NULLS OVER (ORDER BY date)
         ELSE "ripple"."price_open"::float
      END AS ripple
      FROM "bitcoin"
      LEFT JOIN "ripple" ON "bitcoin"."day" = TO_DATE("ripple"."day")),
      
g AS (SELECT "bitcoin"."day" AS date,
      CASE
         WHEN "gold"."GOLD" IS NULL THEN LAG("gold"."GOLD"::float) IGNORE NULLS OVER (ORDER BY date)
         ELSE "gold"."GOLD"::float
      END AS gold 
      FROM "bitcoin"
      LEFT JOIN "gold" ON "bitcoin"."day" = TO_DATE("gold"."DATE",'DD.MM.YYYY')),
      
cl AS (SELECT "bitcoin"."day" AS date,
       CASE
          WHEN "canola"."Open" IS NULL THEN LAG("canola"."Open"::float) IGNORE NULLS OVER (ORDER BY date)
          ELSE "canola"."Open"::float 
       END AS canola
       FROM "bitcoin"
       LEFT JOIN "canola" ON "bitcoin"."day" = TO_DATE("canola"."Date", 'MON DD, YYYY')),
      
ce AS (SELECT "bitcoin"."day" AS date,
       CASE
          WHEN "coffee"."Open" IS NULL THEN LAG("coffee"."Open"::float) IGNORE NULLS OVER (ORDER BY date)
          ELSE "coffee"."Open"::float 
       END AS coffee
       FROM "bitcoin"
       LEFT JOIN "coffee" ON "bitcoin"."day" = TO_DATE("coffee"."Date", 'MON DD, YYYY')),
      
c AS (SELECT "bitcoin"."day" AS date,
       CASE
          WHEN "corn"."Open" IS NULL THEN LAG("corn"."Open"::float) IGNORE NULLS OVER (ORDER BY date)
          ELSE "corn"."Open"::float 
       END AS corn
       FROM "bitcoin"
       LEFT JOIN "corn" ON "bitcoin"."day" = TO_DATE("corn"."Date", 'MON DD, YYYY')),
      
o AS (SELECT "bitcoin"."day" AS date,
       CASE
          WHEN "oil"."Open" IS NULL THEN LAG("oil"."Open"::float) IGNORE NULLS OVER (ORDER BY date)
          ELSE "oil"."Open"::float 
       END AS oil
       FROM "bitcoin"
       LEFT JOIN "oil" ON "bitcoin"."day" = TO_DATE("oil"."Date", 'MON DD, YYYY')),
      
ri AS (SELECT "bitcoin"."day" AS date,
       CASE
          WHEN "rice"."Open" IS NULL THEN LAG("rice"."Open"::float) IGNORE NULLS OVER (ORDER BY date)
          ELSE "rice"."Open"::float 
       END AS rice
       FROM "bitcoin"
       LEFT JOIN "rice" ON "bitcoin"."day" = TO_DATE("rice"."Date", 'MON DD, YYYY')),
      
s AS (SELECT "bitcoin"."day" AS date,
       CASE
          WHEN "soya"."Open" IS NULL THEN LAG(REPLACE("soya"."Open", ',')::float) IGNORE NULLS OVER (ORDER BY date)
          ELSE REPLACE("soya"."Open", ',')::float
       END AS soya
       FROM "bitcoin"
       LEFT JOIN "soya" ON "bitcoin"."day" = TO_DATE("soya"."Date", 'MON DD, YYYY')),
      
w AS (SELECT "bitcoin"."day" AS date,
       CASE
          WHEN "water"."Open" IS NULL THEN LAG("water"."Open"::float) IGNORE NULLS OVER (ORDER BY date)
          ELSE "water"."Open"::float 
       END AS water
       FROM "bitcoin"
       LEFT JOIN "water" ON "bitcoin"."day" = TO_DATE("water"."Date", 'MON DD, YYYY'))

SELECT b.date, b.bitcoin, l.litecoin, e.ethereum, r.ripple, g.gold, cl.canola, ce.coffee, c.corn, o.oil, ri.rice, s.soya, w.water
FROM b
LEFT JOIN l ON b.date = l.date
LEFT JOIN e ON b.date = e.date
LEFT JOIN r ON b.date = r.date
LEFT JOIN g ON b.date = g.date
LEFT JOIN cl ON b.date = cl.date
LEFT JOIN ce ON b.date = ce.date
LEFT JOIN c ON b.date = c.date
LEFT JOIN o ON b.date = o.date
LEFT JOIN ri ON b.date = ri.date  
LEFT JOIN s ON b.date = s.date 
LEFT JOIN w ON b.date = w.date;