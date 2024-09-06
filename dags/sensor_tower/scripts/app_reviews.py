import json
from datetime import datetime, timedelta, date
from airflow.hooks.base import BaseHook
import requests
import pandas as pd
import numpy as np
from google.cloud import bigquery

class SensortowerAppReviews():
    
    def __init__(self):
        
        # self.service_account_json_path = 'airflow-prod-sipher-data-platform-c1999f3f2cc9.json'
        self.service_account_json_path = BaseHook.get_connection("sipher_gcp").extra_dejson["key_path"]
        self.bq_client = bigquery.Client.from_service_account_json(self.service_account_json_path)
        self.bq_dataset = self.bq_client.dataset('tower_sensor_data', project='data-analytics-342807')
        self.job_config = bigquery.LoadJobConfig()
        self.job_config.write_disposition = "WRITE_APPEND"
        
        self.apps_dict = {'com.miHoYo.GenshinImpact':'android',
                          '1517783697':'ios',
                          'com.blizzard.diablo.immortal':'android',
                          '1492005122':'ios',
                          'com.habby.archero':'android',
                          '1453651052':'ios',
                          'com.dxx.firenow':'android',
                          '1528941310':'ios',
                          'com.levelinfinite.hotta.gp':'android',
                          '1601586278':'ios',
                          'com.lilithgames.xgame.gp':'android',
                          '1590319959':'ios',
                          'com.proximabeta.nikke':'android',
                          '1585915174':'ios',
                          'com.hunt.royale':'android',
                          '1537379121':'ios',
                          'com.habby.sssnaker':'android',
                          '1595070036':'ios',
                          'com.HoYoverse.hkrpgoversea':'android',
                          '1599719154':'ios',
                          'com.zigzagame.evertale':'android',
                          '1263365153':'ios',
                          'com.aniplex.fategrandorder.en':'android',
                          '1183802626':'ios',
                          'com.tutapp.herocastlewars':'android',
                          '1555981731':'ios',
                          'com.whoot.games.hot':'android',
                          '1502530494':'ios',
                          'jp.goodsmile.grandsummonersglobal_android':'android',
                          '1329917539':'ios',
                          '1627184882':'ios',
                          'com.artlife.jigsaw.puzzle':'android',
                          '1611547216':'ios',
                          'com.vottzapps.wordle':'android',
                          '1095569891':'ios',
                          'games.urmobi.found.it':'android',
                          '1643547847':'ios',
                          'net.wooga.junes_journey_hidden_object_mystery_game':'android',
                          '1200391796':'ios',
                          'org.smapps.find':'android',
                          '1444585201':'ios',
                          'com.draw.to.pee.bo':'android',
                          'com.playstrom.color.pages':'android',
                          '1643658342':'ios',
                          'com.tfgco.apps.coloring.free.color.by.number':'android',
                          '1317978215':'ios',
                          'com.zigzagame.evertale':'android',
                          '1263365153':'ios',
                          'com.HoYoverse.hkrpgoversea':'android',
                          '1599719154':'ios'
                        }
    
    @classmethod
    def airflow_callable(cls, ds):
        ins = cls()
        ins.run(ds)
   
    def run(self, ds):
        for app_id in self.apps_dict:
            print(app_id)
            app_os = self.apps_dict[app_id]
            ds_nodash = ds.replace('-', '')
            
            reviews = self.get_reviews_by_date(ds, app_os, app_id)
            
            if reviews == '0':
                print(app_id, 'does not have reviews today')
                pass
            else:
                df = self.convert_to_df(reviews)

                #Upload table
                self.bq_table = self.bq_dataset.table(f'top_app_review_{ds_nodash}')
                self.bq_client.load_table_from_dataframe(df, self.bq_table, job_config=self.job_config).result()
                print(self.bq_table)
    
    def get_reviews_by_date(self, ds, app_os, app_id):
        url_path = f'https://api.sensortower.com/v1/{app_os}/review/get_reviews'
        reviews = []
        page = 1
        page_count = 2
        try:
            while page <= page_count:
                print('page: ', page)
                params = {'auth_token': 'ST0_IxfWtBxS_bQy1nsGq7GL2Ee',
                          'app_id': app_id,
                          'country': 'US',
                          'start_date': ds,
                          'end_date': ds,
                          'country': 'US',
                          'limit': 200,
                          'page': page
                         }

                response = requests.get(url_path, params=params)
                response_json = json.loads(response.text)
                reviews += response_json['feedback']

                page_count = response_json['page_count']
                if page == page_count:
                    break
                elif page_count ==0:
                    reviews = '0'
                    break
                else:
                    page += 1
                    continue
        except Exception as e:
            print('page: ',page)
            print('page_count: ',page_count)
            return reviews
        return reviews
    
    def convert_to_df(self,reviews):
        df = pd.DataFrame(reviews)
        df = df.astype({'app_id': str, 'title': str, 'tags': str, 'version': str, 'content': str, 'username': str})
        df = df[['content','version','date','rating','app_id','username','title','country','tags']]
        return df