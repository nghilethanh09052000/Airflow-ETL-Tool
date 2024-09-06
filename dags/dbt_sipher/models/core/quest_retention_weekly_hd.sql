{{config(materialized='table')}}

with s1 as (
    select
        timestamp(createdAt) createdAt,
        min_createdAt,
        quest_userId,
        date_DIFF(date(createdAt), min_createdAt, day) as diff
    from
        `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_claim_quest` o
        left join (
            select
                cast(userId as int64) quest_userId,
                min (date(createdAt)) min_createdAt
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
        cast(FLOOR((diff - 0) / 7) as INT64) as fl
    from
        s1
),
s3 as (
    select
        fl as period,
        count(distinct quest_userId) as quest_userId,
        1 as a
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
        period = 0
)
select
    period,
    quest_userId,
    round((quest_userId - lag) / lag, 2) as churn_rate,
    round(quest_userId / quest_userIds, 2) as drop_
from
    s4,
    s5
