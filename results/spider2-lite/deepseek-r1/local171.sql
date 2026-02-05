WITH `male_la_first_term` AS (
    SELECT `l`.`id_bioguide`, MIN(`lt`.`term_start`) AS `first_start`
    FROM `legislators` `l`
    JOIN `legislators_terms` `lt` ON `l`.`id_bioguide` = `lt`.`id_bioguide`
    WHERE `l`.`gender` = 'M' AND `lt`.`state` = 'LA'
    GROUP BY `l`.`id_bioguide`
), `dec_31_dates` AS (
    SELECT `date`
    FROM `legislation_date_dim`
    WHERE `month_name` = 'December' AND `day_of_month` = 31
), `serving_dates` AS (
    SELECT DISTINCT `m`.`id_bioguide`, `m`.`first_start`, `d`.`date`
    FROM `male_la_first_term` `m`
    JOIN `legislators_terms` `lt` ON `m`.`id_bioguide` = `lt`.`id_bioguide` AND `lt`.`state` = 'LA'
    JOIN `dec_31_dates` `d` ON `d`.`date` BETWEEN `lt`.`term_start` AND `lt`.`term_end`
), `with_years` AS (
    SELECT `id_bioguide`, `first_start`, `date`,
           (CAST(strftime('%Y', `date`) AS INTEGER) - CAST(strftime('%Y', `first_start`) AS INTEGER)) AS `years_elapsed`
    FROM `serving_dates`
), `filtered_years` AS (
    SELECT `id_bioguide`, `years_elapsed`
    FROM `with_years`
    WHERE `years_elapsed` > 30 AND `years_elapsed` < 50
)
SELECT `years_elapsed`, COUNT(DISTINCT `id_bioguide`) AS `num_legislators`
FROM `filtered_years`
GROUP BY `years_elapsed`
ORDER BY `years_elapsed`