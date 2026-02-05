WITH english_courses AS (
  SELECT DISTINCT s."SubjectID"
  FROM "SCHOOL_SCHEDULING"."SCHOOL_SCHEDULING"."SUBJECTS" s
  LEFT JOIN "SCHOOL_SCHEDULING"."SCHOOL_SCHEDULING"."CATEGORIES" c ON s."CategoryID" = c."CategoryID"
  WHERE s."SubjectName" ILIKE '%english%' OR c."CategoryDescription" ILIKE '%english%'
),
completed_english AS (
  SELECT ss."StudentID", ss."Grade"
  FROM "SCHOOL_SCHEDULING"."SCHOOL_SCHEDULING"."STUDENT_SCHEDULES" ss
  JOIN "SCHOOL_SCHEDULING"."SCHOOL_SCHEDULING"."CLASSES" c ON ss."ClassID" = c."ClassID"
  JOIN english_courses ec ON c."SubjectID" = ec."SubjectID"
  WHERE ss."ClassStatus" = 2 AND ss."Grade" IS NOT NULL
),
student_grades AS (
  SELECT s."StudLastName", ce."Grade"
  FROM completed_english ce
  JOIN "SCHOOL_SCHEDULING"."SCHOOL_SCHEDULING"."STUDENTS" s ON ce."StudentID" = s."StudentID"
),
ranked AS (
  SELECT 
    "StudLastName",
    "Grade",
    COUNT(*) OVER (ORDER BY "Grade" DESC) AS "rank",
    COUNT(*) OVER () AS "total_count"
  FROM student_grades
)
SELECT 
  "StudLastName",
  CASE 
    WHEN "rank"::FLOAT / "total_count" <= 0.2 THEN 'First'
    WHEN "rank"::FLOAT / "total_count" <= 0.4 THEN 'Second'
    WHEN "rank"::FLOAT / "total_count" <= 0.6 THEN 'Third'
    WHEN "rank"::FLOAT / "total_count" <= 0.8 THEN 'Fourth'
    ELSE 'Fifth'
  END AS "quintile"
FROM ranked
ORDER BY 
  CASE 
    WHEN "quintile" = 'First' THEN 1
    WHEN "quintile" = 'Second' THEN 2
    WHEN "quintile" = 'Third' THEN 3
    WHEN "quintile" = 'Fourth' THEN 4
    ELSE 5
  END,
  "StudLastName"