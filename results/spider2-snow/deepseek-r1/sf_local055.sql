WITH artist_sales AS (
  SELECT 
    a."ArtistId",
    a."Name",
    COALESCE(SUM(ii."Quantity" * ii."UnitPrice"), 0) AS total_sales
  FROM "CHINOOK"."CHINOOK"."ARTISTS" a
  LEFT JOIN "CHINOOK"."CHINOOK"."ALBUMS" al ON a."ArtistId" = al."ArtistId"
  LEFT JOIN "CHINOOK"."CHINOOK"."TRACKS" t ON al."AlbumId" = t."AlbumId"
  LEFT JOIN "CHINOOK"."CHINOOK"."INVOICE_ITEMS" ii ON t."TrackId" = ii."TrackId"
  GROUP BY a."ArtistId", a."Name"
), top_artist AS (
  SELECT "ArtistId", "Name"
  FROM artist_sales
  ORDER BY total_sales DESC, "Name" ASC
  LIMIT 1
), bottom_artist AS (
  SELECT "ArtistId", "Name"
  FROM artist_sales
  ORDER BY total_sales ASC, "Name" ASC
  LIMIT 1
), customer_spending AS (
  SELECT 
    c."CustomerId",
    SUM(CASE WHEN a."ArtistId" = ta."ArtistId" THEN ii."Quantity" * ii."UnitPrice" ELSE 0 END) AS top_spent,
    SUM(CASE WHEN a."ArtistId" = ba."ArtistId" THEN ii."Quantity" * ii."UnitPrice" ELSE 0 END) AS bottom_spent
  FROM "CHINOOK"."CHINOOK"."CUSTOMERS" c
  INNER JOIN "CHINOOK"."CHINOOK"."INVOICES" i ON c."CustomerId" = i."CustomerId"
  INNER JOIN "CHINOOK"."CHINOOK"."INVOICE_ITEMS" ii ON i."InvoiceId" = ii."InvoiceId"
  INNER JOIN "CHINOOK"."CHINOOK"."TRACKS" t ON ii."TrackId" = t."TrackId"
  INNER JOIN "CHINOOK"."CHINOOK"."ALBUMS" al ON t."AlbumId" = al."AlbumId"
  INNER JOIN "CHINOOK"."CHINOOK"."ARTISTS" a ON al."ArtistId" = a."ArtistId"
  CROSS JOIN top_artist ta
  CROSS JOIN bottom_artist ba
  GROUP BY c."CustomerId"
), top_avg AS (
  SELECT AVG(top_spent) AS avg_top
  FROM customer_spending
  WHERE top_spent > 0
), bottom_avg AS (
  SELECT AVG(bottom_spent) AS avg_bottom
  FROM customer_spending
  WHERE bottom_spent > 0
)
SELECT ABS(COALESCE(top_avg.avg_top,0) - COALESCE(bottom_avg.avg_bottom,0)) AS diff
FROM top_avg, bottom_avg