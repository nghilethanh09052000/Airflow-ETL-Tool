from airflow.utils.task_group import TaskGroup
from sensor_tower.operators import DemoGraphicEndpointOperator


"""
    Naming Convention:
    - Group tasks: group_tasks_get_**
    - Task: task_get_**
    **: Requirement
"""

COUNTRY_CODES = {
        "AU": "Australia",
        "BR": "Brazil",
        "CA": "Canada",
        "DE": "Germany",
        "ES": "Spain",
        "FR": "France",
        "GB": "United Kingdom",
        "IN": "India",
        "IT": "Italy",
        "JP": "Japan",
        "KR": "South Korea",
        "US": "US"
}


def group_tasks_get_quaterly_demographic_data_in_app_ids(
        ds: str,
        group_task_id,
        gcs_bucket,
        gcs_prefix,
        http_conn_id
    ):

    app_data = [
        {"name": "lonely_survivor", "ios": 1637393009, "android": "com.cobby.lonelysurvivor"},
        {"name": "wizard_hero", "ios": 1627205864, "android": "com.homecookedgames.magero"},
        {"name": "暴走小蝦米", "ios": 1661057340, "android": "com.lldxsmdj.android"},
        {"name": "zombie_waves", "ios": 6443760593, "android": "com.ddup.zombiewaves.zw"},
        {"name": "infinite_magicraid", "ios": 1625632561, "android": "com.ihgames.im.android.google"},
        {"name": "heroes_vs._hordes:_survivor", "ios": 1608898173, "android": "com.swiftgames.survival"},
        {"name": "omniheroes", "ios": 1620283683, "android": "com.omnidream.ohs"},
        {"name": "survivor.io", "ios": 1528941310, "android": "com.dxx.firenow"},
        {"name": "honkai_star_rail", "ios": 1599719154, "android": "com.HoYoverse.hkrpgoversea"},
        {"name": "abyss_-_roguelike_action_rpg", "ios": 6443793989, "android": "com.titans.abyss"},
        {"name": "hunt_royale:_action_rpg_battle", "ios": 1537379121, "android": "com.hunt.royale"},
        {"name": "dislyte", "ios": 1590319959, "android": "com.lilithgames.xgame.gp"},
        {"name": "metal_slug:_awakening", "ios": 1621102178, "android": "com.vng.sea.metalslug"},
        {"name": "tower_of_fantasy", "ios": 1601586278, "android": "com.levelinfinite.hotta.gp"},
        {"name": "brawl_star", "ios": 1229016807, "android": "com.supercell.brawlstars"},
        {"name": "t3_arena", "ios": 1602814337, "android": "com.xd.t3.global"},
        {"name": "gunfire_reborn", "ios": 1606703078, "android": "com.duoyihk.m2m1"},
        {"name": "diablo_immortal", "ios": 1492005122, "android": "com.blizzard.diablo.immortal"},
        {"name": "dungeon_hunter_5", "ios": 885823239, "android": "com.gameloft.android.ANMP.Gloft5DHM"},
        {"name": "genshin_impact", "ios": 1517783697, "android": "com.miHoYo.GenshinImpact"},
        {"name": "battle_chasers:_nightwar", "ios": 1455088996, "android": "com.hg.bcnw"},
        {"name": "moonshades:_dungeon_crawler_rpg", "ios": 1492040231, "android": ""},
        {"name": "torchlight", "ios": 1593130084, "android": "com.xd.TLglobal"},
        {"name": "dungeon_hunter_6", "ios": 1664335636, "android": "com.goatgames.dhs.gb.gp"},
    ]

    with TaskGroup(
        group_task_id,
        tooltip="Get Countries In API and use Quarterly Date Granuality"
    ) as group_task_id:
        tasks = []
        for country in list(COUNTRY_CODES.keys()):
            task = DemoGraphicEndpointOperator(
                    ds=ds,
                    task_id=f'get_quarterly_demographics_data_country_{country}',
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=gcs_prefix,
                    http_conn_id=http_conn_id,
                    os=['ios', 'android'],
                    app_ids=app_data,
                    country=country,
                    date_granularity='quarterly'
                )

            tasks.append(task)
        
    return group_task_id    


