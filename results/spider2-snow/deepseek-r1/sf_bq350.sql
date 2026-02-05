SELECT DISTINCT m."id", m."drugType", m."hasBeenWithdrawn"
FROM "OPEN_TARGETS_PLATFORM_1"."PLATFORM"."MOLECULE" m
INNER JOIN "OPEN_TARGETS_PLATFORM_1"."PLATFORM"."KNOWNDRUGSAGGREGATED" k
ON m."id" = k."drugId"
WHERE m."isApproved" = TRUE
AND m."blackBoxWarning" = TRUE
AND k."prefName" IN ('Keytruda', 'Vioxx', 'Premarin', 'Humira')