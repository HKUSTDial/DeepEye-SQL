SELECT major_category, SUM(value) AS incidents
FROM `bigquery-public-data.london_crime.crime_by_lsoa`
WHERE borough = 'Barking and Dagenham'
GROUP BY major_category
ORDER BY incidents DESC
LIMIT 3