from typing import Dict, List, Sequence, Any,  Literal
from airflow.utils.task_group import TaskGroup
from sensor_tower.operators import GetReviewsEndpointOperator
from utils.constants import COUNTRY_CODE

"""
    Naming Convention:
    - Group tasks: group_tasks_get_**
    - Task: task_get_**
    **: Requirement
"""

"""
    app_ids type: Dict[str , Literal['android', 'ios']]:
    {
        'abc': 'android',
        '123': 'ios'
    }
"""


def task_get_get_review_us_country_on_specific_app_ids(
    ds: str,
    task_id: str,
    gcs_bucket: str,
    gcs_prefix: str,
    http_conn_id: str
):

    app_ids: Dict[str , Literal['android', 'ios']] = {
        'com.miHoYo.GenshinImpact': 'android',
        '1517783697': 'ios',
        'com.blizzard.diablo.immortal': 'android',
        '1492005122': 'ios',
        'com.habby.archero': 'android',
        '1453651052': 'ios',
        'com.dxx.firenow': 'android',
        '1528941310': 'ios',
        'com.levelinfinite.hotta.gp': 'android',
        '1601586278': 'ios',
        'com.lilithgames.xgame.gp': 'android',
        '1590319959': 'ios',
        'com.proximabeta.nikke': 'android',
        '1585915174': 'ios',
        'com.hunt.royale': 'android',
        '1537379121': 'ios',
        'com.habby.sssnaker': 'android',
        '1595070036': 'ios',
        'com.HoYoverse.hkrpgoversea': 'android',
        '1599719154': 'ios',
        'com.zigzagame.evertale': 'android',
        '1263365153': 'ios',
        'com.aniplex.fategrandorder.en': 'android',
        '1183802626': 'ios',
        'com.tutapp.herocastlewars': 'android',
        '1555981731': 'ios',
        'com.whoot.games.hot': 'android',
        '1502530494': 'ios',
        'jp.goodsmile.grandsummonersglobal_android': 'android',
        '1329917539': 'ios',
        '1627184882': 'ios',
        'com.artlife.jigsaw.puzzle': 'android',
        '1611547216': 'ios',
        'com.vottzapps.wordle': 'android',
        '1095569891': 'ios',
        'games.urmobi.found.it': 'android',
        '1643547847': 'ios',
        'net.wooga.junes_journey_hidden_object_mystery_game': 'android',
        '1200391796': 'ios',
        'org.smapps.find': 'android',
        '1444585201': 'ios',
        'com.draw.to.pee.bo': 'android',
        'com.playstrom.color.pages': 'android',
        '1643658342': 'ios',
        'com.tfgco.apps.coloring.free.color.by.number': 'android',
        '1317978215': 'ios',
        'com.zigzagame.evertale': 'android',
        '1263365153': 'ios',
        'com.HoYoverse.hkrpgoversea': 'android',
        '1599719154': 'ios'
        }

    return GetReviewsEndpointOperator(
        ds=ds,
        task_id=task_id,
        gcs_bucket=gcs_bucket,
        gcs_prefix=gcs_prefix,
        http_conn_id=http_conn_id,
        os=['ios', 'android'],
        app_ids=app_ids,
        countries=['US'],
        rating_filter='',
        search_term='',
        username='',
        limit=200,
        page=1
    )
