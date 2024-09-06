{{config(materialized='table')}}

with s1 as (
    select
        dt,
        min_dt,
        quest_userId,
        TIMESTAMP_DIFF(dt, min_dt, day) as diff
    from
        `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_claim_quest` o
        left join (
            select
                cast(userId as int64) quest_userId,
                min (dt) min_dt
            from
                `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_claim_quest`
            group by
                1
        ) s on s.quest_userId = cast(o.userId as int64)
    where
        true
),
s2 as (
    select
        quest_userId,
        diff,
        cast(FLOOR((diff - 0) / 1) as INT64) as fl
    from
        s1
),
s3 as (
    select
        concat('day ', fl) as period,
        count(distinct quest_userId) as quest_userId,
        '1' as a
    from
        s2
    group by
        1
    order by
        1
),
s4 as (
    select
        *,
        LAG(quest_userId) OVER (
            PARTITION BY a
            order by
                period asc
        ) as lag
    from
        s3
),
s5 as (
    select
        quest_userId as quest_userIds
    from
        s4
    where
        period = 'day 0'
)
select
    period,
    quest_userId,
    round((quest_userId - lag) * 100 / lag, 2) as churn_rate,
    round(quest_userId * 100 / quest_userIds, 2) as drop_
from
    s4,
    s5
