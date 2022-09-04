SELECT *
FROM view_event
WHERE mag in (SELECT MAX(mag) FROM Properties);
