    SELECT
      DISTINCT 
        DATE_TRUNC(DATE(_pt),WEEK(MONDAY)) + 6  AS week_end,
        test,
        variant,
        COUNT(DISTINCT COALESCE(cast(combined_regi_id AS STRING),agent_id)) as agents
   
    FROM
        `nyt-eventtracker-prd.et.page`,
        UNNEST(ab_exposes)
    WHERE
     
        test LIKE "%Wirecutter_Regi_%"
        AND source_app LIKE '%wirecutter%'
        AND DATE(_pt) between '2023-01-02' and '2023-02-08'
        -- AND combined_regi_id IS NOT NULL
    GROUP BY 1,2,3
    ORDER BY 1,2,3