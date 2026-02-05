WITH `BestArtist` AS (
  SELECT `a`.`ArtistId`
  FROM `artists` `a`
  JOIN `albums` `al` ON `a`.`ArtistId` = `al`.`ArtistId`
  JOIN `tracks` `t` ON `al`.`AlbumId` = `t`.`AlbumId`
  JOIN `invoice_items` `ii` ON `t`.`TrackId` = `ii`.`TrackId`
  GROUP BY `a`.`ArtistId`
  ORDER BY SUM(`ii`.`UnitPrice` * `ii`.`Quantity`) DESC
  LIMIT 1
), `CustomerSpending` AS (
  SELECT `c`.`CustomerId`, `c`.`FirstName`, SUM(`ii`.`UnitPrice` * `ii`.`Quantity`) AS `total_spent`
  FROM `customers` `c`
  JOIN `invoices` `i` ON `c`.`CustomerId` = `i`.`CustomerId`
  JOIN `invoice_items` `ii` ON `i`.`InvoiceId` = `ii`.`InvoiceId`
  JOIN `tracks` `t` ON `ii`.`TrackId` = `t`.`TrackId`
  JOIN `albums` `al` ON `t`.`AlbumId` = `al`.`AlbumId`
  JOIN `BestArtist` `ba` ON `al`.`ArtistId` = `ba`.`ArtistId`
  GROUP BY `c`.`CustomerId`, `c`.`FirstName`
)
SELECT `FirstName`, `total_spent`
FROM `CustomerSpending`
WHERE `total_spent` < 1
ORDER BY `FirstName`