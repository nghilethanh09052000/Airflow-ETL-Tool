import pandas as pd
import logging
from aws_to_gcs.scripts.aws_file_base import AWSFileGCS


class AwsBilling(AWSFileGCS):
    def __init__(self, ds):
        super().__init__(ds)

        self.input_folder_name_list = [
            "sipher-g1-billing",
            "sipher-marketplace-billing",
            "sipher-blockchain-billing",
            "sipher-game-production-billing",
            "metaverse-billing"
        ]
        self.output_folder_name = "aws-billing"
        self.columns = {
            "sipher-g1-billing": [
                "identity/LineItemId",
                "bill/BillingPeriodStartDate",
                "lineItem/UsageStartDate",
                "lineItem/ProductCode",
                "lineItem/UnblendedCost",
                "lineItem/BlendedCost",
                "lineItem/LineItemDescription",
                "lineItem/LineItemType",
                "product/region",
                "resourceTags/user:eks:cluster-name",
                "resourceTags/user:project",
                "resourceTags/user:type"
            ],
            "sipher-marketplace-billing": [
                "identity/LineItemId",
                "bill/BillingPeriodStartDate",
                "lineItem/UsageStartDate",
                "lineItem/ProductCode",
                "lineItem/UnblendedCost",
                "lineItem/BlendedCost",
                "lineItem/LineItemDescription",
                "lineItem/LineItemType",
                "product/region",
                "resourceTags/user:eks:cluster-name",
                "resourceTags/user:project",
                "resourceTags/user:type"
            ],
            "sipher-blockchain-billing": [
                "identity/LineItemId",
                "bill/BillingPeriodStartDate",
                "lineItem/UsageStartDate",
                "lineItem/ProductCode",
                "lineItem/UnblendedCost",
                "lineItem/BlendedCost",
                "lineItem/LineItemDescription",
                "lineItem/LineItemType",
                "product/region",
                "resourceTags/user:eks:cluster-name",
                "resourceTags/user:project",
                "resourceTags/user:type"
            ],
            "sipher-game-production-billing": [
                "identity/LineItemId",
                "bill/BillingPeriodStartDate",
                "lineItem/UsageStartDate",
                "lineItem/ProductCode",
                "lineItem/UnblendedCost",
                "lineItem/BlendedCost",
                "lineItem/LineItemDescription",
                "lineItem/LineItemType",
                "product/region",
                "resourceTags/user:eks:cluster-name",
                "resourceTags/user:project",
                "resourceTags/user:type"
            ],
            "metaverse-billing": [
                "identity/LineItemId",
                "bill/BillingPeriodStartDate",
                "lineItem/UsageStartDate",
                "lineItem/ProductCode",
                "lineItem/UnblendedCost",
                "lineItem/BlendedCost",
                "lineItem/LineItemDescription",
                "lineItem/LineItemType",
                "product/region",
                "resourceTags/user:eks:cluster-name",
                "resourceTags/user:project",
                "resourceTags/user:type"
            ]
        }

    
    def run(self):
        self.init_gcs_client()
        object_file_type = ".csv"
        for input_folder_name in self.input_folder_name_list:
            self.input_folder_name = input_folder_name
            prefix_path = f"{self.input_folder_name}"

            object_list = self.get_object_list(prefix_path)

            prefix_path = f"{self.input_folder_name}/dt={self.date}/"
            object_list = self.get_object_list(
                prefix_path
            )  # the new object_list when filter the prefix_path with the max date

            for object_path in object_list:
                object_name = self.find_object_path_derived(object_path, type='object_name')
                if input_folder_name == 'metaverse-billing':
                    object_type = 'metaverse-billing'
                else:
                    object_type = "-".join(object_name.split("-")[0:-1])
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
        result_df = pd.read_csv(file_path + object_file_type)

        result_df = result_df.reset_index().drop(columns="index")
        result_df = result_df.astype(str)
        result_df = result_df[columns]
        result_df = result_df.drop_duplicates()

        result_df.to_parquet(file_path + ".parquet", index=False, compression="snappy")
        logging.info(f"Cleaned file_path: {file_path}.parquet")