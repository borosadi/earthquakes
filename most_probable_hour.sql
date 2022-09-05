WITH
m as (
    SELECT 
        CASE
            WHEN mag < 1 THEN '0-1 magnitude'
            WHEN mag < 2 THEN '1-2 magnitude'
            WHEN mag < 3 THEN '2-3 magnitude'
            WHEN mag < 4 THEN '3-4 magnitude'
            WHEN mag < 5 THEN '4-5 magnitude'
            WHEN mag < 6 THEN '5-6 magnitude'
            WHEN mag > 6 THEN '>6 magnitude'
            ELSE 'invalid magnitude value'
        END AS magnitude,
        HOUR(time) as hour
    FROM view_event
    ),

c as (
    SELECT DISTINCT
        magnitude,
        hour,
        COUNT(magnitude) OVER (PARTITION BY hour, magnitude) as mag_count,
        COUNT(magnitude) OVER (PARTITION BY hour, magnitude) / COUNT(magnitude) OVER (PARTITION BY magnitude) as mag_prob
    FROM m
    WHERE magnitude != 'invalid magnitude value'
    ),

mc as (
    SELECT
        magnitude,
        MAX(mag_count) as max_mag_count
    FROM c
    GROUP BY magnitude
    )

SELECT
    c.hour as hour,
    c.magnitude as magnitude,
    c.mag_prob as probability,
    c.mag_count as count,
    CASE
        WHEN mc.magnitude IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_most_probable
FROM mc
RIGHT OUTER JOIN c
ON mc.magnitude = c.magnitude
AND mc.max_mag_count = c.mag_count
