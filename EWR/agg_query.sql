-- Gives me everything (session channel, headline, etc)

-- CREATE OR REPLACE TABLE  `nyt-bigquery-beta-workspace.jose_data.ewr_new_sc`
-- -- -- -- -- PARTITION BY date

-- AS

WITH test_agents AS (

 SELECT 

        DATE(_pt) as date,
        test,
        variant,
        COALESCE(cast(combined_regi_id AS STRING),agent_id) as user_id,
        pg.pageview_id,
        wirecutter.asset.headline as headline

    FROM
        `nyt-eventtracker-prd.et.page` AS pg,
        UNNEST(ab_exposes)

    WHERE 1 = 1
        AND wirecutter.asset.id IN ("RE10255", "RE11784", "RE9079", "RE9994", "RE898", "RE11541", "RE560", "RE12339", "RE11420", "RE784", "RE11229", "RE874", "RE462", "RE12229","RE674")
        AND test = 'Wirecutter_cwv_ewr_0323'
        AND variant IN ("0_Control", "1_EWR")
        AND source_app LIKE '%wirecutter%'
        AND DATE(_pt) BETWEEN '2023-04-11' AND '2023-04-24'
        AND COALESCE(cast(combined_regi_id AS STRING),agent_id) NOT IN -- Dedup agents 
                (
                    SELECT
                        COALESCE(cast(combined_regi_id AS STRING),agent_id)
                    FROM `nyt-eventtracker-prd.et.page`, unnest(ab_exposes)
                        WHERE 1=1
                          AND DATE(_pt) BETWEEN '2023-04-11' AND '2023-04-24'
                          AND source_app LIKE '%wirecutter%'
                          AND COALESCE(cast(combined_regi_id AS STRING),agent_id) IS NOT NULL
                          AND test = 'Wirecutter_cwv_ewr_0323'
                        GROUP BY 1
                        HAVING COUNT(DISTINCT variant) > 1
                )

 ),

--   real_agents AS (

-- -- look at agents in the duration of the test that visited the 15 pages

-- -- where user id in 

-- select distinct  user_id
--  from `nyt-bigquery-beta-workspace.wirecutter_data.channel`
--  where 
--  1=1 
--  AND user_id IN (

--      SELECT DISTINCT user_id
--      FROM test_agents
--  )
-- AND  object_id IN  ("RE10255", "RE11784", "RE9079", "RE9994", "RE898", "RE11541", "RE560", "RE12339", "RE11420", "RE784", "RE11229", "RE874", "RE462", "RE12229","RE674")
-- AND date BETWEEN '2023-04-11' AND '2023-04-24'

--  )

--  select count(*)
--  from real_agents
all_clicks as (

     SELECT
         DATE(_pt) as date,
        COALESCE(cast(pg.combined_regi_id AS STRING),pg.agent_id) as user_id,
        COUNT(int.module.element.name) AS pclicks,
        pg.pageview_id,
        wirecutter.asset.id as asset_id

    FROM
        `nyt-eventtracker-prd.et.page` AS pg,
        unnest(interactions) AS int

    WHERE 1=1
        AND DATE(_pt) BETWEEN '2023-04-11' AND '2023-04-24'
        AND source_app LIKE '%wirecutter%'
        AND int.module.element.name LIKE '%outbound_product%'
        AND wirecutter.asset.id IN ("RE10255", "RE11784", "RE9079", "RE9994", "RE898", "RE11541", "RE560", "RE12339", "RE11420", "RE784", "RE11229", "RE874", "RE462", "RE12229", "RE674")
    GROUP BY 1,2,4,5

 ),
carousel as (

    SELECT
         DATE(_pt) as date,
        COALESCE(cast(pg.combined_regi_id AS STRING),pg.agent_id) as user_id,
        COUNT(int.module.element.name) AS pclicks,
        pg.pageview_id,
        wirecutter.asset.id

    FROM
        `nyt-eventtracker-prd.et.page` AS pg,
        unnest(interactions) AS int

    WHERE 1=1
        AND DATE(_pt) BETWEEN '2023-04-11' AND '2023-04-24'
        AND source_app LIKE '%wirecutter%'
        AND int.module.element.name LIKE '%outbound_product%'
        AND wirecutter.asset.id IN ("RE10255", "RE11784", "RE9079", "RE9994", "RE898", "RE11541", "RE560", "RE12339", "RE11420", "RE784", "RE11229", "RE874", "RE462", "RE12229","RE674")
        AND  int.module.context = "ewr"

    GROUP BY 1,2,4,5
),

callout as (
 SELECT
         DATE(_pt) as date,
        COALESCE(cast(pg.combined_regi_id AS STRING),pg.agent_id) as user_id,
         COUNT(int.module.element.name) AS pclicks,
        pg.pageview_id,
        wirecutter.asset.id

    FROM
        `nyt-eventtracker-prd.et.page` AS pg,
        unnest(interactions) AS int

    WHERE 1=1
        AND DATE(_pt) BETWEEN '2023-04-11' AND '2023-04-24'
        AND source_app LIKE '%wirecutter%'
        AND int.module.element.name LIKE '%outbound_product%'
        AND wirecutter.asset.id IN ("RE10255", "RE11784", "RE9079", "RE9994", "RE898", "RE11541", "RE560", "RE12339", "RE11420", "RE784", "RE11229", "RE874", "RE462", "RE12229","RE674")
        AND  int.module.context = "inline"
    GROUP BY 1,2,4,5
),

in_text as (
 SELECT
         DATE(_pt) as date,
        COALESCE(cast(pg.combined_regi_id AS STRING),pg.agent_id) as user_id,
        COUNT(int.module.element.name) AS pclicks,
        pg.pageview_id,
         wirecutter.asset.id

    FROM
        `nyt-eventtracker-prd.et.page` AS pg,
        unnest(interactions) AS int

    WHERE 1=1
        AND DATE(_pt) BETWEEN '2023-04-11' AND '2023-04-24'
        AND source_app LIKE '%wirecutter%'
        AND int.module.element.name LIKE '%outbound_product%'
        AND int.module.element.name LIKE '%link%'
        AND wirecutter.asset.id IN ("RE10255", "RE11784", "RE9079", "RE9994", "RE898", "RE11541", "RE560", "RE12339", "RE11420", "RE784", "RE11229", "RE874", "RE462", "RE12229","RE674")
        -- AND  int.module.context is null
     GROUP BY 1,2,4,5
),

deals as (
 SELECT
         DATE(_pt) as date,
        COALESCE(cast(pg.combined_regi_id AS STRING),pg.agent_id) as user_id,
        COUNT(int.module.element.name) AS pclicks,
        pg.pageview_id,
        wirecutter.asset.id

    FROM
        `nyt-eventtracker-prd.et.page` AS pg,
        unnest(interactions) AS int

    WHERE 1=1
        AND DATE(_pt) BETWEEN '2023-04-11' AND '2023-04-24'
        AND source_app LIKE '%wirecutter%'
        AND int.module.element.name LIKE '%outbound_product%'
        AND int.module.element.name LIKE '%deal%'
        AND wirecutter.asset.id IN ("RE10255", "RE11784", "RE9079", "RE9994", "RE898", "RE11541", "RE560", "RE12339", "RE11420", "RE784", "RE11229", "RE874", "RE462", "RE12229","RE674")
        -- AND  int.module.context is null

      GROUP BY 1,2,4,5
),

all_non_carousel as (
    SELECT
         DATE(_pt) as date,
        COALESCE(cast(pg.combined_regi_id AS STRING),pg.agent_id) as user_id,
        COUNT(int.module.element.name) AS pclicks,
        pg.pageview_id,
        wirecutter.asset.id

    FROM
        `nyt-eventtracker-prd.et.page` AS pg,
        unnest(interactions) AS int

    WHERE 1=1
        AND DATE(_pt) BETWEEN '2023-04-11' AND '2023-04-24'
        AND source_app LIKE '%wirecutter%'
        AND int.module.element.name LIKE '%outbound_product%'
        AND wirecutter.asset.id IN ("RE10255", "RE11784", "RE9079", "RE9994", "RE898", "RE11541", "RE560", "RE12339", "RE11420", "RE784", "RE11229", "RE874", "RE462", "RE12229","RE674")
        AND ( int.module.context = "inline" or int.module.context is null )
    GROUP BY 1,2,4,5
),
 session_channel AS (

    SELECT 
    session_channel_2,-- in the session 
    channel_2, -- what led to this page 
    COALESCE(user_id,agent_id) as user_id,
    pageview_id,
    agent_day_session_pageview_index,
    device

FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel`
WHERE date BETWEEN '2023-04-11' AND '2023-04-24'

--  ),
 
--  device as (
-- SELECT 
--      COALESCE(user_id,agent_id) as user_id,
--      pageview_id,
--      device
--     FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel`
--      where 
--      1=1 
--      AND date BETWEEN '2023-04-11' AND '2023-04-24'
  
    -- AND  object_id IN  ("RE10255", "RE11784", "RE9079", "RE9994", "RE898", "RE11541", "RE560", "RE12339", "RE11420", "RE784", "RE11229", "RE874", "RE462", "RE12229","RE674")
    -- AND date BETWEEN '2023-04-11' AND '2023-04-24'

    --  )

 )

SELECT 
        -- ta.date,
        ta.variant,
        sc.device,
        session_channel_2,
        channel_2,
        ta.headline,
        -- ac.asset.id,
        COUNT(DISTINCT ta.user_id) as user_count,
        SUM(COALESCE(ac.pclicks,0)) as all_pclicks,
        SUM(COALESCE(ca.pclicks,0)) as carousel_pclicks,
        SUM(COALESCE(co.pclicks,0)) as callout_pclicks,
        SUM(COALESCE(it.pclicks,0))as in_text_pclicks,
        SUM(COALESCE(d.pclicks,0)) as deals_pclicks,
        SUM(COALESCE(anc.pclicks,0)) as non_carousel_pclicks
   
        
FROM test_agents ta 
    LEFT JOIN all_clicks ac ON ac.pageview_id = ta.pageview_id AND ac.user_id = ta.user_id
    LEFT JOIN carousel ca ON ca.pageview_id = ta.pageview_id AND ca.user_id = ta.user_id
    LEFT JOIN callout co ON co.pageview_id = ta.pageview_id AND co.user_id = ta.user_id
    LEFT JOIN in_text it ON it.pageview_id = ta.pageview_id AND it.user_id = ta.user_id
    LEFT JOIN deals d ON d.pageview_id = ta.pageview_id AND d.user_id = ta.user_id
    LEFT JOIN all_non_carousel anc ON anc.pageview_id = ta.pageview_id AND anc.user_id = ta.user_id
    JOIN session_channel sc ON sc.pageview_id = ta.pageview_id AND sc.user_id = ta.user_id


-- WHERE ac.asset.id 
GROUP BY 1,2,3--,3,4
ORDER BY 1,2, 4 desc, 5 desc