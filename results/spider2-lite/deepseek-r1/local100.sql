WITH shahrukh_pid AS (
    SELECT `PID` FROM `Person` WHERE TRIM(`Name`) = 'Shahrukh Khan'
),
shahrukh_movies AS (
    SELECT DISTINCT `MID` FROM `M_Cast` WHERE TRIM(`PID`) = (SELECT `PID` FROM shahrukh_pid)
),
shahrukh_coactors AS (
    SELECT DISTINCT TRIM(`PID`) AS `PID` FROM `M_Cast` 
    WHERE `MID` IN (SELECT `MID` FROM shahrukh_movies)
),
coactor_movies AS (
    SELECT DISTINCT `MID` FROM `M_Cast`
    WHERE TRIM(`PID`) IN (SELECT `PID` FROM shahrukh_coactors)
      AND `MID` NOT IN (SELECT `MID` FROM shahrukh_movies)
),
shahrukh_number_2_actors AS (
    SELECT DISTINCT TRIM(`PID`) AS `PID` FROM `M_Cast`
    WHERE `MID` IN (SELECT `MID` FROM coactor_movies)
      AND TRIM(`PID`) NOT IN (SELECT `PID` FROM shahrukh_coactors)
)
SELECT COUNT(*) FROM shahrukh_number_2_actors;