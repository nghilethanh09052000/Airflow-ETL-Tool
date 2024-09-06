import requests
import pandas as pd
import json
import time
from datetime import timezone, datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from airflow.hooks.base import BaseHook
from airflow.models import Variable

BIGQUERY_PROJECT = Variable.get("bigquery_project")

class EtherscanWalletBalance():
    def __init__(self):
        self.polygon_wallets = {
            "Clone Sipher OpenSea Commission MSIG": "0x1299461a6dc8E755F7299cC221B29776d7eDb663",
            "Dopa JSC Polygon Gnosis SALA": "0xf5D57a3EC1fB77f12D1FB9bf761a93A3ddD7c35B",
            "Dopa JSC Polygon Gnosis OPEX": "0xb7273C93095F9597FcBFC48837e852F4CF8b39b2",
        }

        self.polygon_tokens = {
            "USDT": {
                "address": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
                "decimal": 6,
            },
            "USDC": {
                "address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                "decimal": 6,
            },
            "wBTC": {
                "address": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6",
                "decimal": 8,
            },
            "WETH": {
                "address": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
                "decimal": 18,
            },
            "COMP": {
                "address": "0x8505b9d2254A7Ae468c0E9dd10Ccea3A837aef5c",
                "decimal": 18,
            },
        }

        self.polygon_api_key = Variable.get("polygon_api_key")
        self.polygon_list = []

        self.etherscan_wallets = {
            "Gnosis Sipher Seed & Partnership Round MSIG": "0xF5c935c8E6bd741c74f8633a106c0CA33E3c4faf",
            "Gnosis $SIPHER General Management MSIG": "0x11986f428b22c011082820825ca29B21a3C11295",
            "Gnosis NFT OpenSea Commission MSIG": "0x1299461a6dc8E755F7299cC221B29776d7eDb663",
            "Gnosis Sipher B2B Guilds MSIG": "0x94B1a79C1a2a3Fedb40EF3af44DEc1DEd8Bc26f4",
            "Gnosis Sipher NFT Sale MSIG": "0x1390047A78a029383d0ADcC1adB5053b8fA3243F",
            "1102 Equity Fundraising": "0x128114f00540a0f59b12DE5e2BaE354FcEdf0aa2",
            "Athereal.eth Public Wallet": "0x3e8c6676eef25c7b18a7ac24271075f735c79a16",
            "Sipher Cold Wallet Trezor": "0x3BC15f3601eA7b65a9A8E7f8C776d4Ab5e2Bc002",
        }

        self.etherscan_tokens = {
            "USDT": {
                "address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                "decimal": 6,
            },
            "USDC": {
                "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "decimal": 6,
            },
            "cUSDC": {
                "address": "0x39AA39c021dfbaE8faC545936693aC917d5E7563",
                "decimal": 8,
            },
            "cUSDT": {
                "address": "0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9",
                "decimal": 8,
            },
            "wBTC": {
                "address": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
                "decimal": 8,
            },
            "stETH": {
                "address": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
                "decimal": 18,
            },
            "WETH": {
                "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "decimal": 18,
            },
            "wstETH": {
                "address": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
                "decimal": 18,
            },
            "COMP": {
                "address": "0xc00e94Cb662C3520282E6f5717214004A7f26888",
                "decimal": 18,
            },
        }

        self.etherscan_api_key = Variable.get("etherscan_api_key")
        self.etherscan_list = []

        self.service_account_json_path = BaseHook.get_connection(
            "sipher_gcp"
        ).extra_dejson["key_path"]
        self.credentials = service_account.Credentials.from_service_account_file(
            self.service_account_json_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.bigquery_project = BIGQUERY_PROJECT
        self.job_config = bigquery.LoadJobConfig()
        self.job_config.write_disposition = "WRITE_APPEND"

    def run(self):
        self.get_polygon_wallet_tokens_balance()
        self.get_etherscan_wallet_eth_balance()
        self.get_etherscan_wallet_tokens_balance()

        polygon_df = pd.DataFrame(self.polygon_list)
        etherscan_df = pd.DataFrame(self.etherscan_list)
        df_all = pd.concat([etherscan_df, polygon_df])

        #upload to bq
        self.client = bigquery.Client(
            project=BIGQUERY_PROJECT, credentials=self.credentials
        )
        self.bq_dataset = self.client.dataset(
            "raw_etherscan", project=BIGQUERY_PROJECT
        )
        self.bq_table = self.bq_dataset.table("etherscan_polygon_important_wallet_accounts_balance")
        self.client.load_table_from_dataframe(
            df_all, self.bq_table, job_config=self.job_config
        ).result()

    def get_polygon_wallet_tokens_balance(self):
        module = "account"
        action = "tokenbalance"
        for wallet_name in self.polygon_wallets:
            for token_name in self.polygon_tokens:
                wallet_address = self.polygon_wallets[wallet_name]
                token_address = self.polygon_tokens[token_name]["address"]
                url = f"https://api.polygonscan.com/api?module={module}&action={action}&address={wallet_address}&contractaddress={token_address}&sort=asc&apikey={self.polygon_api_key}&tag=latest"
                response = requests.get(url)
                response_json = response.json()

                tmp_dict = dict()
                tmp_dict["wallet_address"] = wallet_address
                tmp_dict["wallet_name"] = wallet_name
                tmp_dict["token_address"] = token_address
                tmp_dict["token_symbol"] = token_name
                tmp_dict["value"] = response_json["result"]
                tmp_dict["decimal"] = self.polygon_tokens[token_name]["decimal"]
                tmp_dict["network"] = "polygon"
                tmp_dict["timestamp"] = int(time.time())
                tmp_dict["date"] = datetime.strptime(datetime.now(timezone.utc).strftime("%Y-%m-%d"), '%Y-%m-%d')

                self.polygon_list.append(tmp_dict)
                time.sleep(10)

    def get_etherscan_wallet_eth_balance(self):
        module = "account"
        action = "balance"
        for wallet_name in self.etherscan_wallets:
            wallet_address = self.etherscan_wallets[wallet_name]
            url = f'https://api.etherscan.io/api?module={module}&action={action}&address={wallet_address}&tag=latest&apikey={self.etherscan_api_key}'
            response = requests.get(url)
            response_json = response.json()
            
            tmp_dict = dict()
            tmp_dict["wallet_address"] = wallet_address
            tmp_dict["wallet_name"] = wallet_name
            tmp_dict["token_address"] = ''
            tmp_dict["token_symbol"] = 'ETH'
            tmp_dict["value"] = response_json['result']
            tmp_dict["decimal"] = 18
            tmp_dict["network"] = 'etherscan'
            tmp_dict["timestamp"] = int(time.time())
            tmp_dict['date'] = datetime.strptime(datetime.now(timezone.utc).strftime("%Y-%m-%d"), '%Y-%m-%d')

            self.etherscan_list.append(tmp_dict)
            time.sleep(10)
        
    def get_etherscan_wallet_tokens_balance(self):
        module = 'account'
        action = 'tokenbalance'
        for wallet_name in self.etherscan_wallets:
            for token_name in self.etherscan_tokens:
                wallet_address = self.etherscan_wallets[wallet_name]
                token_address = self.etherscan_tokens[token_name]['address']
                url = f'https://api.etherscan.io/api?module={module}&action={action}&address={wallet_address}&contractaddress={token_address}&sort=asc&apikey={self.etherscan_api_key}&tag=latest'    
                response = requests.get(url)
                response_json = response.json()
                
                tmp_dict = dict()
                tmp_dict["wallet_address"] = wallet_address
                tmp_dict["wallet_name"] = wallet_name
                tmp_dict["token_address"] = token_address
                tmp_dict["token_symbol"] = token_name
                tmp_dict["value"] = response_json['result']
                tmp_dict["decimal"] = self.etherscan_tokens[token_name]['decimal']
                tmp_dict["network"] = 'etherscan'
                tmp_dict["timestamp"] = int(time.time())
                tmp_dict['date'] = datetime.strptime(datetime.now(timezone.utc).strftime("%Y-%m-%d"), '%Y-%m-%d')
                
                self.etherscan_list.append(tmp_dict)
                time.sleep(10)