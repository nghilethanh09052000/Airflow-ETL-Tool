{{config(materialized='table')}}

with a as (
    select
        *,
        lag(o_time) over (
            partition by quest_userId
            order by
                o_time
        ) as previous_day,
        date_diff(
            o_time,
            lag(o_time) over (
                partition by quest_userId
                order by
                    o_time
            ),
            day
        ) as gap
    from
        (
            select
                cast(userId as int64) quest_userId,
                date_trunc(dt, day) as o_time,
                date_trunc(date(min_dt), day) as f_day
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
            order by
                dt
        ) s
),
f as(
    select
        o_time,
        lag(count(distinct quest_userId)) over (
            order by
                o_time
        ) as begining ,--- Số  lúc bắt đầu,
        count(distinct quest_userId) as ending ,
        --- Số  hiện tại,
        count(
            distinct case when o_time <> f_day
            and (
                previous_day is null
                or gap > 1
            ) then quest_userId end
        ) as returns ,-- Số  không hoạt động ở thời điểm trước n nhưng hoạt động ở thời điểm n (thời điểm được tính),
        count(
            distinct case when o_time = f_day then quest_userId end
        ) as news ,-- Số  mới,
        count(
            distinct case when o_time <> f_day
            and gap = 1 then quest_userId end
        ) as retention ,--- Số retention,
        lag(count(distinct quest_userId)) over (
            order by
                o_time
        ) - count(
            distinct case when o_time <> f_day
            and gap = 1 then quest_userId end
        ) as churn_driver --- Số  tạm thời rời bỏ. BEGINING - RETENTION
        -- select *
    from
        a
    group by
        1
    order by
        o_time
)
select
    format_date('%Y-%m-%d', o_time) as o_time,
    begining,
    ending,
    returns,
    news,
    retention,
    churn_driver
from
    f