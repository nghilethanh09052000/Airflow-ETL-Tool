import json
import pandas as pd
import csv
import logging
from aws_to_gcs.scripts.aws_file_base import AWSFileGCS
import re


class AtherId(AWSFileGCS):
    def __init__(self, ds):
        super().__init__(ds)

        self.input_folder_name = "ather-id"
        self.output_folder_name = "ather-id"
        self.columns = {
            "user": [
                "id",
                "subscribeEmail",
                "cognitoSub",
                "updatedAt",
                "avatarImage",
                "createdAt",
                "email",
                "name",
                "refCode",
                "isVerified",
                "isBanned",
                "bio",
                "bannerImage",
            ],
            "wallet": ["address", "cognitoSub", "createdAt", "userId", "updatedAt"],
            "total": ["updated_date", "total"],
            "cognito": [
                "Username", 
                "Attributes",
                "UserCreateDate",
                "UserLastModifiedDate",
                "Enabled",
                "UserStatus",
                "connected_wallets",
                "sub",
                "email_verified",
                "user_id",
                "email",
                "identities",
                "name",
                "providerName",
                "providerType",
                "issuer",
                "primary",
                "dateCreated"
            ],
        }

        self.folder_type_list = ["type=user", "type=wallet", "type=cognito"]

    
    def run(self):
        self.init_gcs_client()

        prefix_path = f"{self.input_folder_name}/dt={self.date}/"
        object_list = self.get_object_list(
            prefix_path
        )

        for object_path in object_list:
            object_file_type = ".json"
            detect_type = self.find_object_path_derived(object_path, type='detect_type')
            if detect_type in self.folder_type_list:
                object_type = self.find_object_path_derived(object_path, type='object_type')
                if object_type == "cognito":
                    object_file_type = ".txt"
                    object_name = self.find_object_path_derived(object_path, type='object_name')
                    logging.info(f"Object Name: {object_name}")

                    blob_name=f"{self.input_folder_name}/dt={self.date}/type={object_type}/{object_name}{object_file_type}"
                    blob = self.input_bucket.blob(
                        blob_name=blob_name
                    )
                    logging.info(f"Blob_name: {blob_name}")

                    object_name = "cognito"
                else:
                    object_name = self.find_object_path_derived(object_path, type='object_name')
                    logging.info(f"Object Name: {object_name}")

                    blob_name=f"{self.input_folder_name}/dt={self.date}/type={object_type}/{object_name}{object_file_type}"
                    blob = self.input_bucket.blob(
                        blob_name=blob_name
                    )
                    logging.info(f"Blob_name: {blob_name}")

            else:
                object_type = object_name = detect_type
                logging.info(f"Object Name: {object_name}")

                blob_name=f"{self.input_folder_name}/dt={self.date}/{object_name}{object_file_type}"
                blob = self.input_bucket.blob(
                    blob_name=blob_name
                )
                logging.info(f"Blob_name: {blob_name}")

            file_path = self.download_and_get_object_local_path(
                object_name, object_file_type, blob
            )
            logging.info(f"File_path: {file_path}")
            col_object = re.sub(r'[^a-zA-Z]', '', object_name)
            columns = self.columns[col_object]
            self.clean_file_content_to_parquet(file_path, object_file_type, columns, object_name)
            self.upload_file_to_gcs(object_type, object_name, object_file_type, file_path)
    
    
    def find_object_path_derived(self, object_path, type=None):
        if type == 'object_name':
            return object_path.split("/")[3].split(".")[0]
        elif type == 'object_type':
            return object_path.split("/")[2].split("=")[1]
        elif type == 'detect_type':
            return object_path.split("/")[2].split(".")[0]
        else:
            logging.error("Cannot derive object_path")

    
    def clean_file_content_to_parquet(self, file_path, object_file_type, columns, object_name):
        logging.info(".....START CLEANING.....")

        if object_name == "total":
            content = open(file_path + object_file_type, "r").read()
            j_content = json.loads("[" + content + "]")
            result_df = pd.DataFrame(j_content)
            result_df["updated_date"] = self.date

        elif object_name == "cognito":
            with open(file_path + object_file_type, "r") as f:
                content = [row[0] for row in csv.reader(f, delimiter="\t")]
            for indx, string in enumerate(content):
                content[indx] = (
                    content[indx]
                    .replace('"Name":', "")
                    .replace(',"Value"', "")
                    .replace("custom:", "")
                    .replace("},{", ",")
                )
            result = [json.loads("{%s}" % item[1:-1]) for item in content]
            df = pd.DataFrame(result)
            df_attributes = df["Attributes"].explode(["Attributes"]).apply(pd.Series)
            identity_columns = df_attributes.apply(self.extract_identity_columns, axis=1)
            temp = pd.concat([df_attributes, identity_columns], axis=1, join="inner")
            result_df = pd.concat([df, temp], axis=1, join="inner")
        else:
            content   = open(file_path + object_file_type, "r").read()
            result_df = pd.DataFrame()
            j_content = json.loads("[" + content.replace("}\n{", "},\n{") + "]")
            for item in pd.DataFrame(j_content)["Item"]:
                item_df = (
                    pd.DataFrame(item).ffill().bfill().drop_duplicates(keep="first")
                )
                result_df = pd.concat([result_df, item_df])

        result_df = result_df.reset_index().drop(columns="index")
        result_df = result_df.reindex(columns=columns, fill_value=None)
        result_df = result_df[columns]
        result_df = result_df.astype(str)
        result_df.to_parquet(file_path + ".parquet", index=False, compression="snappy")
        logging.info(f"Cleaned file_path: {file_path}.parquet")

    def extract_identity_columns(self, row):
        identities_str = row["identities"]
        if pd.isna(identities_str) or not identities_str:  
            return pd.Series({
                "providerName": None,
                "providerType": None,
                "issuer"      : None,
                "primary"     : None,
                "dateCreated" : None
            })
        else:
            identities = json.loads(identities_str.replace("'", '"'))
            if identities:  
                return pd.Series({
                    "providerName": identities[0].get("providerName"),
                    "providerType": identities[0].get("providerType"),
                    "issuer"      : identities[0].get("issuer"),
                    "primary"     : identities[0].get("primary"),
                    "dateCreated" : identities[0].get("dateCreated")
                })
            else:
                return pd.Series({
                    "providerName": None,
                    "providerType": None,
                    "issuer"      : None,
                    "primary"     : None,
                    "dateCreated" : None
                })