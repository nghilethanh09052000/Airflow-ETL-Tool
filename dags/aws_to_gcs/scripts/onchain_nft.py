import json
import pandas as pd
import logging
from aws_to_gcs.scripts.aws_file_base import AWSFileGCS


class AwsOnchainNFT(AWSFileGCS):
    def __init__(self, ds):
        super().__init__(ds)

        self.input_folder_name = "nft-assets"
        self.output_folder_name = "onchain-nft-assets"
        self.columns = {
            "Lootbox": [
                "owner",
                "tokenId",
                "chainId",
                "id",
                "type",
                "collectionId",
                "value",
            ],
            "Sculpture": [
                "owner",
                "tokenId",
                "chainId",
                "id",
                "type",
                "collectionId",
                "value",
            ],
            "Spaceship_Parts": [
                "owner",
                "tokenId",
                "chainId",
                "id",
                "type",
                "collectionId",
                "value",
            ],
            "Spaceship": [
                "owner",
                "tokenId",
                "chainId",
                "id",
                "type",
                "collectionId",
                "value",
            ],
            "NEKO": [
                "owner",
                "tokenId",
                "chainId",
                "name",
                "id",
                "type",
                "collectionId",
                "value",
            ],
            "INU": [
                "owner",
                "tokenId",
                "chainId",
                "name",
                "id",
                "type",
                "collectionId",
                "value",
            ],
        }
        self.object_type_dict = {
            "sipher-lootbox": "Lootbox",
            "sipher-sculpture": "Sculpture",
            "sipher-spaceship-part": "Spaceship_Parts",
            "sipher-spaceship": "Spaceship",
            "sipherian-flash": "NEKO",
            "sipherian-surge": "INU",
        }

    
    def run(self):
        self.init_gcs_client()
        object_file_type = ".json"
        prefix_path = f"{self.input_folder_name}/dt={self.date}/"
        object_list = self.get_object_list(
            prefix_path
        )

        for object_path in object_list:
            object_name = self.find_object_path_derived(object_path, type='object_name')
            object_type = "-".join(object_name.split("-")[0:3])
            object_type = self.object_type_dict[object_type]

            logging.info(f"Object Name: {object_name}")
            logging.info(f"Object Type: {object_type}")

            blob_name=f"{self.input_folder_name}/dt={self.date}/{object_name}{object_file_type}"
            blob = self.input_bucket.blob(
                blob_name=blob_name
            )
            logging.info(f"Blob_name: {blob_name}")

            file_path = self.download_and_get_object_local_path(
                object_name, object_file_type, blob
            )
            logging.info(f"File_path: {file_path}")

            columns = self.columns[object_type]
            self.clean_file_content_to_parquet(file_path, object_file_type, columns)
            self.upload_file_to_gcs(
                object_type, object_name, object_file_type, file_path
            )

    
    def find_object_path_derived(self, object_path, type=None):
        if type == 'object_name':
            return object_path.split("/")[2].split(".")[0]
        else:
            logging.error("Cannot derive object_path")

    
    def clean_file_content_to_parquet(self, file_path, object_file_type, columns):
        logging.info(".....START CLEANING.....")
        content = open(file_path + object_file_type, "r").read()
        content = content.replace("\n", ",")
        content = "[" + content[0:-1] + "]"
        j_content = json.loads(content, strict=False)

        result_df = pd.DataFrame(j_content)

        result_df = result_df.reset_index().drop(columns="index")
        result_df = result_df[columns]

        result_df = result_df.astype(str)

        result_df = result_df.drop_duplicates()

        result_df.to_parquet(file_path + ".parquet", index=False, compression="snappy")
        logging.info(f"Cleaned file_path: {file_path}.parquet")