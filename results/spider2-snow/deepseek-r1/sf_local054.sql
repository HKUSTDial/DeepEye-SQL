WITH best_selling_artist AS (
  SELECT 
    ar."ArtistId"
  FROM 
    "CHINOOK"."CHINOOK"."ARTISTS" ar
    JOIN "CHINOOK"."CHINOOK"."ALBUMS" al ON ar."ArtistId" = al."ArtistId"
    JOIN "CHINOOK"."CHINOOK"."TRACKS" t ON al."AlbumId" = t."AlbumId"
    JOIN "CHINOOK"."CHINOOK"."INVOICE_ITEMS" ii ON t."TrackId" = ii."TrackId"
  GROUP BY 
    ar."ArtistId"
  ORDER BY 
    SUM(ii."UnitPrice" * ii."Quantity") DESC
  LIMIT 1
),
albums_by_best_artist AS (
  SELECT 
    "AlbumId"
  FROM 
    "CHINOOK"."CHINOOK"."ALBUMS"
  WHERE 
    "ArtistId" = (SELECT "ArtistId" FROM best_selling_artist)
),
tracks_from_albums AS (
  SELECT 
    "TrackId"
  FROM 
    "CHINOOK"."CHINOOK"."TRACKS"
  WHERE 
    "AlbumId" IN (SELECT "AlbumId" FROM albums_by_best_artist)
),
customer_spending AS (
  SELECT 
    c."CustomerId",
    c."FirstName",
    SUM(ii."UnitPrice" * ii."Quantity") AS total_spent
  FROM 
    "CHINOOK"."CHINOOK"."INVOICE_ITEMS" ii
    JOIN "CHINOOK"."CHINOOK"."INVOICES" i ON ii."InvoiceId" = i."InvoiceId"
    JOIN "CHINOOK"."CHINOOK"."CUSTOMERS" c ON i."CustomerId" = c."CustomerId"
  WHERE 
    ii."TrackId" IN (SELECT "TrackId" FROM tracks_from_albums)
  GROUP BY 
    c."CustomerId", c."FirstName"
  HAVING 
    total_spent < 1.00
)
SELECT 
  "FirstName",
  total_spent
FROM 
  customer_spending
ORDER BY 
  "FirstName"