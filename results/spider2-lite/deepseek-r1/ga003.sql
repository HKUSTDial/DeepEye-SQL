WITH completion_events AS (
    SELECT 
        events.user_pseudo_id,
        events.event_timestamp,
        MAX(CASE WHEN ep.key = 'board_type' THEN ep.value.string_value END) AS board_type,
        MAX(CASE WHEN ep.key = 'score' THEN COALESCE(ep.value.int_value, ep.value.float_value) END) AS score
    FROM `firebase-public-project.analytics_153293282.events_20180915` AS events,
    UNNEST(event_params) AS ep
    WHERE EXISTS (SELECT 1 FROM UNNEST(event_params) AS ep2 
                  WHERE ep2.key = 'game_mode' AND ep2.value.string_value = 'quick_play')
      AND events.event_date = '20180915'
    GROUP BY events.user_pseudo_id, events.event_timestamp
    HAVING board_type IS NOT NULL AND score IS NOT NULL
)
SELECT 
    board_type,
    AVG(score) AS average_score
FROM completion_events
GROUP BY board_type
ORDER BY average_score DESC