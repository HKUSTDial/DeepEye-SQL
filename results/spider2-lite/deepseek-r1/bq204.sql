SELECT `user`, SUM(clicks) as total_clicks
FROM (
  SELECT `user`, COUNT(*) as clicks
  FROM `bigquery-public-data.eclipse_megamovie.photos_v_0_1`
  GROUP BY `user`
  UNION ALL
  SELECT `user`, COUNT(*) as clicks
  FROM `bigquery-public-data.eclipse_megamovie.photos_v_0_2`
  GROUP BY `user`
  UNION ALL
  SELECT `user`, COUNT(*) as clicks
  FROM `bigquery-public-data.eclipse_megamovie.photos_v_0_3`
  GROUP BY `user`
)
GROUP BY `user`
ORDER BY total_clicks DESC
LIMIT 1