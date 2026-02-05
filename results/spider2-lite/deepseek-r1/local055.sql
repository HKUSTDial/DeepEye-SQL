WITH `artist_sales` AS (
    SELECT 
        `a`.`ArtistId`,
        `a`.`Name`,
        COALESCE(SUM(`ii`.`Quantity` * `ii`.`UnitPrice`), 0) AS `total_sales`
    FROM `artists` `a`
    LEFT JOIN `albums` `al` ON `a`.`ArtistId` = `al`.`ArtistId`
    LEFT JOIN `tracks` `t` ON `al`.`AlbumId` = `t`.`AlbumId`
    LEFT JOIN `invoice_items` `ii` ON `t`.`TrackId` = `ii`.`TrackId`
    GROUP BY `a`.`ArtistId`, `a`.`Name`
),
`top_artist` AS (
    SELECT `ArtistId`, `Name`
    FROM `artist_sales`
    ORDER BY `total_sales` DESC, `Name` ASC
    LIMIT 1
),
`bottom_artist` AS (
    SELECT `ArtistId`, `Name`
    FROM `artist_sales`
    ORDER BY `total_sales` ASC, `Name` ASC
    LIMIT 1
),
`customer_artist_spending` AS (
    SELECT 
        `i`.`CustomerId`,
        `a`.`ArtistId`,
        SUM(`ii`.`Quantity` * `ii`.`UnitPrice`) AS `amount_spent`
    FROM `invoices` `i`
    JOIN `invoice_items` `ii` ON `i`.`InvoiceId` = `ii`.`InvoiceId`
    JOIN `tracks` `t` ON `ii`.`TrackId` = `t`.`TrackId`
    JOIN `albums` `al` ON `t`.`AlbumId` = `al`.`AlbumId`
    JOIN `artists` `a` ON `al`.`ArtistId` = `a`.`ArtistId`
    GROUP BY `i`.`CustomerId`, `a`.`ArtistId`
),
`top_customer_avg` AS (
    SELECT AVG(`amount_spent`) AS `avg_top`
    FROM `customer_artist_spending`
    WHERE `ArtistId` = (SELECT `ArtistId` FROM `top_artist`)
),
`bottom_customer_avg` AS (
    SELECT AVG(`amount_spent`) AS `avg_bottom`
    FROM `customer_artist_spending`
    WHERE `ArtistId` = (SELECT `ArtistId` FROM `bottom_artist`)
)
SELECT ABS(
    COALESCE((SELECT `avg_top` FROM `top_customer_avg`), 0) -
    COALESCE((SELECT `avg_bottom` FROM `bottom_customer_avg`), 0)
) AS `absolute_difference`