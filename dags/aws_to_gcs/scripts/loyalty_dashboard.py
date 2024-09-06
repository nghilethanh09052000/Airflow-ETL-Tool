import json
import pandas as pd
import logging
from aws_to_gcs.scripts.aws_file_base import AWSFileGCS


class Loyalty(AWSFileGCS):
    def __init__(self, ds):
        super().__init__(ds)

        self.input_folder_name = "dashboard"
        self.output_folder_name = "loyalty-dashboard"
        self.columns = {
            "airdrop": [
                "id",
                "merkleRoot",
                "proof",
                "leaf",
                "claimer",
                "addressContract",
                "totalAmount",
                "type",
                "startTime",
                "vestingInterval",
                "name",
                "shortDescription",
                "description",
                "numberOfVestingPoint",
                "createdAt",
                "updatedAt",
            ],
            "base_spaceship": [
                "id",
                "name",
                "code",
                "shortDescription",
                "description",
                "external_url",
                "createdAt",
                "updatedAt",
            ],
            "cart": ["id", "userId", "isOrdered", "createdAt", "updatedAt"],
            "cart_item": [
                "id",
                "type",
                "itemId",
                "itemColor",
                "itemSize",
                "itemQty",
                "createdAt",
                "updatedAt",
                "cartId",
            ],
            "cart_item_redemption": [
                "id",
                "wallet",
                "amount",
                "createdAt",
                "updatedAt",
                "cartItemId",
            ],
            "erc1155_lootbox": [
                "id",
                "tokenId",
                "name",
                "shortDescription",
                "description",
                "external_url",
                "image",
                "createdAt",
            ],
            "erc1155_lootbox_attribute": [],
            "erc1155_sculpture": [
                "id",
                "tokenId",
                "name",
                "shortDescription",
                "description",
                "external_url",
                "image",
                "createdAt",
                "updatedAt",
            ],
            "erc1155_sculpture_attribute": [],
            "erc1155_spaceship": [
                "id",
                "tokenId",
                "name",
                "image",
                "createdAt",
                "updatedAt",
                "baseId",
                "rarity",
                "rarityScore",
            ],
            "erc1155_spaceship_attribute": [
                "id",
                "trait_type",
                "value",
                "createdAt",
                "updatedAt",
                "erc1155Id",
            ],
            "erc1155_spaceship_part": [
                "id",
                "tokenId",
                "name",
                "shortDescription",
                "description",
                "external_url",
                "image",
                "createdAt",
                "updatedAt",
            ],
            "erc1155_spaceship_part_attribute": [
                "id",
                "trait_type",
                "value",
                "createdAt",
                "updatedAt",
                "erc1155Id",
            ],
            "erc721_character": [
                "id",
                "nftId",
                "name",
                "image",
                "currentEmotion",
                "proof",
                "origin",
                "race",
                "createdAt",
                "updatedAt",
            ],
            "erc721_character_attribute": [
                "id",
                "trait_type",
                "value",
                "createdAt",
                "erc721Id",
            ],
            "erc721_character_emotion": [
                "id",
                "emotion",
                "image",
                "createdAt",
                "erc721Id",
            ],
            "history_log": [
                "id",
                "userId",
                "email",
                "action",
                "feature",
                "description",
                "createdAt",
            ],
            "history_log_metadata": [
                "id",
                "key",
                "label",
                "value",
                "historyLogId",
                "createdAt",
            ],
            "image_url": [
                "id",
                "color",
                "default",
                "front",
                "back",
                "left",
                "right",
                "top",
                "bot",
                "createdAt",
                "updatedAt",
                "airdropId",
                "itemId",
            ],
            "item": [
                "id",
                "merchItem",
                "name",
                "type",
                "shortDescription",
                "description",
                "size",
                "color",
                "createdAt",
                "updatedAt",
            ],
            "log_admin": ["id", "input", "output", "type", "createdAt", "updatedAt"],
            "burned": [
                "id",
                "to",
                "batchID",
                "amount",
                "salt",
                "type",
                "createdAt",
                "updatedAt",
            ],
            "canceled": ["id", "signature", "type", "createdAt", "updatedAt"],
            "claimable_lootbox": [
                "id",
                "publicAddress",
                "quantity",
                "tokenId",
                "createdAt",
                "updatedAt",
                "propertyLootboxId",
            ],
            "log_claim_lootbox": [
                "id",
                "publicAddress",
                "quantity",
                "tokenId",
                "createdAt",
                "updatedAt",
                "isRandom",
            ],
            "log_claim_quest": [
                "id",
                "questTitle",
                "questPlatform",
                "totalXp",
                "totalNanochips",
                "baseXp",
                "baseNanochips",
                "promotionXp",
                "promotionNanochips",
                "xpBonusPercentage",
                "nanochipsBonusPercentage",
                "userId",
                "email",
                "twitterId",
                "discordId",
                "createdAt",
            ],
            "log_distribute_lootbox": [
                "id",
                "from",
                "to",
                "quantity",
                "tokenId",
                "expiredDate",
                "createdAt",
                "updatedAt",
            ],
            "log_open_lootbox": [
                "id",
                "publicAddress",
                "atherId",
                "lootboxId",
                "spaceshipPartIds",
                "createdAt",
                "updatedAt",
            ],
            "log_quest_status": [
                "id",
                "status",
                "createdAt",
                "updatedAt",
                "userQuestId",
            ],
            "log_scrap_spaceship_parts": [
                "id",
                "publicAddress",
                "atherId",
                "spaceshipPartTokenIds",
                "caseTypeNumbers",
                "newSpaceshipPartTokenId",
                "newSpaceshipPartCaseTypeNumber",
                "createdAt",
                "updatedAt",
            ],
            "log_spaceship": [
                "id",
                "tokenId",
                "publicAddress",
                "atherId",
                "name",
                "partTokenIds",
                "action",
                "createdAt",
                "updatedAt",
            ],
            "lootbox": [
                "id",
                "publicAddress",
                "quantity",
                "tokenId",
                "mintable",
                "createdAt",
                "updatedAt",
                "propertyLootboxId",
            ],
            "merchandise": [
                "id",
                "publicAddress",
                "tier",
                "merchItem",
                "quantity",
                "quantityShipped",
                "isShipped",
                "shippable",
                "createdAt",
                "updatedAt",
                "itemId",
            ],
            "notification": [
                "id",
                "recipientId",
                "metadata",
                "isRead",
                "isShown",
                "type",
                "message",
                "createdAt",
                "updatedAt",
            ],
            "order": [
                "userId",
                "shippingAddress",
                "shippingCity",
                "shippingState",
                "shippingRegion",
                "shippingZipcode",
                "firstName",
                "lastName",
                "email",
                "phone",
                "shippingCode",
                "trackingLink",
                "shippingFee",
                "currency",
                "status",
                "createdAt",
                "updatedAt",
                "cartId",
                "id",
            ],
            "pending_mint": [
                "id",
                "to",
                "batchID",
                "amount",
                "salt",
                "deadline",
                "status",
                "type",
                "signature",
                "createdAt",
                "updatedAt",
            ],
            "quest": [
                "id",
                "code",
                "title",
                "description",
                "slotNumber",
                "bannerUrl",
                "rewardXp",
                "rewardNanochips",
                "startTime",
                "endTime",
                "requiredLevel",
                "isActive",
                "isDeactivated",
                "prerequisites",
                "createdAt",
                "updatedAt",
                "baseId",
            ],
            "quest_base": [
                "id",
                "name",
                "type",
                "platform",
                "defaultStatus",
                "frequency",
                "createdAt",
                "updatedAt",
            ],
            "quest_metadata": ["id", "key", "value", "questId", "createdAt"],
            "quest_promotion": [],
            "quest_referral_rewarded": [
                "id",
                "userId",
                "refId",
                "createdAt",
                "updatedAt",
            ],
            "rule_claim_box": ["id", "tokenId", "createdAt", "updatedAt"],
            "sculpture_transaction": [
                "id",
                "event",
                "tokenId",
                "amount",
                "ownerAddress",
                "createdAt",
                "updatedAt",
            ],
            "sipher_collection": [
                "id",
                "name",
                "collectionSlug",
                "chainId",
                "collectionType",
                "category",
                "floorPrice",
                "totalVolume",
                "marketCap",
                "totalSupply",
                "totalSales",
                "description",
                "logoImage",
                "bannerImage",
                "siteUrl",
                "isVerified",
                "createdAt",
                "updatedAt",
                "race",
            ],
            "spaceship_ownership": [
                "id",
                "publicAddress",
                "quantity",
                "mintable",
                "erc1155SpaceshipId",
                "createdAt",
                "updatedAt",
            ],
            "spaceship_part": [
                "id",
                "publicAddress",
                "quantity",
                "tokenId",
                "rarity",
                "rarityName",
                "caseType",
                "spaceshipName",
                "mintable",
                "createdAt",
                "updatedAt",
                "propertySpaceshipPartId",
            ],
            "tracked_block": ["id", "type", "tracked", "createdAt", "updatedAt"],
            "twitter_follower": [
                "id",
                "followerId",
                "twitterId",
                "createdAt",
                "updatedAt",
            ],
            "twitter_liking_user": [],
            "twitter_retweeted_user": [],
            "user_management_info": [
                "id",
                "name",
                "email",
                "discordId",
                "twitterId",
                "wallets",
                "sipherNfts",
                "sipherTokens",
                "referrer",
                "active",
                "createdAt",
                "updatedAt",
            ],
            "user_quest": [
                "id",
                "userId",
                "status",
                "progress",
                "createdAt",
                "updatedAt",
                "questId",
                "logClaimQuestId",
            ],
            "user_quest_info": ["userId", "xp", "nanochips", "createdAt", "updatedAt"],
            "user_quest_stats": [
                "id",
                "userQuestInfoUserId",
                "oneTimeQuest",
                "likeAndRetweetQuest",
                "dailyCheckinQuest",
                "currentTokenHoldingStreak",
                "longestTokenHoldingStreak",
                "checkedUserQuestIds",
                "lastDailyCompletionDate",
                "createdAt",
                "updatedAt",
            ],
        }

    
    def run(self):
        self.init_gcs_client()
        object_file_type = ".json"
        prefix_path = f"{self.input_folder_name}/dt={self.date}/"
        object_list = self.get_object_list(
            prefix_path
        )  # the new object_list when filter the prefix_path with the max date

        for object_path in object_list:
            object_name = self.find_object_path_derived(object_path, type='object_name')
            object_type = self.find_object_path_derived(object_path, type='object_type')
            if object_type in [
                "quest_promotion",
                "erc1155_lootbox_attribute",
                "erc1155_sculpture_attribute",
                "twitter_liking_user",
                "twitter_retweeted_user",
            ]:
                continue
            logging.info(f"Object Name: {object_name}")
            logging.info(f"Object Type: {object_type}")

            blob_name=f"{self.input_folder_name}/dt={self.date}/type={object_type}/{object_name}{object_file_type}"
            blob = self.input_bucket.blob(
                blob_name=blob_name
            )
            logging.info(f"Blob_name: {blob_name}")

            file_path = self.download_and_get_object_local_path(
                object_name, object_file_type, blob
            )
            logging.info(f"File_path: {file_path}")

            columns = self.columns[object_type]
            self.clean_file_content_to_parquet(
                file_path, columns, object_type, object_file_type
            )
            self.upload_file_to_gcs(
                object_type, object_name, object_file_type, file_path
            )

    
    def find_object_path_derived(self, object_path, type=None):
        if type == 'object_name':
            return object_path.split("/")[3].split(".")[0]
        elif type == 'object_type':
            return object_path.split("/")[2].split("=")[1]
        else:
            logging.error("Cannot derive object_path")
    
    
    def clean_file_content_to_parquet(
        self, file_path, columns, object_type, object_file_type
    ):
        logging.info(".....START CLEANING.....")
        content = open(file_path + object_file_type, "r").read()
        content = content.replace("\\\\", "\\").replace("\\n", "\n")
        j_content = json.loads(content, strict=False)

        result_df = pd.DataFrame(j_content)

        result_df = result_df.reset_index().drop(columns="index")
        result_df = result_df.astype(str)

        if object_type in ["burned", "pending_mint"]:
            result_df_single = result_df.loc[result_df["amounts"].isin(["[]"])]
            result_df_batch = result_df.loc[-result_df["amounts"].isin(["[]"])]

            result_df_batch["batchIDs"] = (
                result_df_batch["batchIDs"].str.strip("[]").str.split(",")
            )
            result_df_batch["amounts"] = (
                result_df_batch["amounts"].str.strip("[]").str.split(",")
            )
            result_df_batch = result_df_batch.explode(["amounts", "batchIDs"])
            result_df_batch["amount"] = result_df_batch["amounts"].str.strip()
            result_df_batch["batchID"] = result_df_batch["batchIDs"].str.strip()

            result_df = result_df_batch.append(result_df_single, ignore_index=True)

        else:
            pass

        result_df = result_df[columns]
        result_df = result_df.astype(str)

        result_df.to_parquet(file_path + ".parquet", index=False, compression="snappy")
        logging.info(f"Cleaned file_path: {file_path}.parquet")