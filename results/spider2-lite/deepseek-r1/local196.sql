WITH first_rental AS (
  SELECT customer_id, rental_id AS first_rental_id
  FROM (
    SELECT 
      customer_id,
      rental_id,
      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY payment_date, rental_id) AS rn
    FROM payment
    WHERE rental_id IS NOT NULL
  ) t
  WHERE rn = 1
),
customer_payments AS (
  SELECT customer_id, SUM(amount) AS total_amount
  FROM payment
  GROUP BY customer_id
),
customer_rental_counts AS (
  SELECT customer_id, COUNT(*) AS total_rentals
  FROM rental
  GROUP BY customer_id
),
customer_data AS (
  SELECT 
    fr.customer_id,
    f.rating,
    cp.total_amount,
    crc.total_rentals
  FROM first_rental fr
  JOIN rental r ON fr.first_rental_id = r.rental_id
  JOIN inventory i ON r.inventory_id = i.inventory_id
  JOIN film f ON i.film_id = f.film_id
  JOIN customer_payments cp ON fr.customer_id = cp.customer_id
  JOIN customer_rental_counts crc ON fr.customer_id = crc.customer_id
)
SELECT 
  rating,
  AVG(total_amount) AS avg_total_amount,
  AVG(total_rentals - 1) AS avg_subsequent_rentals
FROM customer_data
GROUP BY rating
ORDER BY rating;