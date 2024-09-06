import csv
import logging
import time
from datetime import datetime
from io import BytesIO, StringIO

import discord
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from utils.constants import PandasDtypes
from utils.data_upload.bigquery_upload import BigQueryDataUpload

EXECUTION_ENVIRONMENT = Variable.get("execution_environment", default_var="dev")


class Discord:
    def __init__(self, **kwargs):

        self.service_account_json_path = BaseHook.get_connection(
            "sipher_gcp"
        ).extra_dejson["key_path"]

        self.access = Variable.get(f"discord_token", deserialize_json=True)

        self.dataset = "raw_social" if EXECUTION_ENVIRONMENT == "production" else "tmp3"

    def create_guild_stats_df_from_guild_attribute(self, guild):
        logging.info("Create DataFrame...")
        schema = {
            "created_at": PandasDtypes.DATETIME,
            "name": PandasDtypes.STRING,
            "id": PandasDtypes.INT64,
            "description": PandasDtypes.STRING,
            "owner": PandasDtypes.STRING,
            "owner_id": PandasDtypes.STRING,
            "member_count": PandasDtypes.INT64,
        }

        df = pd.DataFrame(
            [
                [
                    str(guild.created_at),
                    guild.name,
                    guild.id,
                    guild.description,
                    str(guild.owner),
                    guild.owner_id,
                    guild.member_count,
                ]
            ],
            columns=schema.keys(),
        )

        df = df.astype(schema)
        return df

    def upload_data(self, df, table_name, dataset_name=None):
        dataset = dataset_name or self.dataset
        self.uploader = BigQueryDataUpload(
            self.service_account_json_path, dataset, table_name
        )
        self.uploader.load_dataframe(df)

    @classmethod
    def get_guild_stats_from_event(cls, **kwargs):
        ins = cls(**kwargs)
        intents = discord.Intents.all()
        client = discord.Client(intents=intents)
        errors = []

        @client.event
        async def on_ready():
            try:
                logging.info("Listening to guild info...")
                guild = client.get_guild(ins.access.get("guild_id"))
                df = ins.create_guild_stats_df_from_guild_attribute(guild)
                date_suffix = datetime.strftime(datetime.today(), "%Y%m%d")

                ins.upload_data(df, f"discord_profile_stats__{date_suffix}")
                logging.info("Done")
            except Exception as e:
                logging.error(f"Adding `{e}` to errors")
                errors.append(e)
            await client.close()

        client.run(ins.access.get("token"))
        if errors:
            raise ValueError(
                f"There was at least 1 ERROR during the event loop. List of errors {errors}"
            )

    @classmethod
    def get_guild_member_user_info(cls, **kwargs):
        ins = cls(**kwargs)
        intents = discord.Intents.all()
        member_cache_flags = discord.MemberCacheFlags.all()
        client = discord.Client(intents=intents, member_cache_flags=member_cache_flags)
        errors = []
        file_out = StringIO()
        file_out_writer = csv.writer(file_out)
        file_out_writer.writerow(
            [
                "created_at",
                "joined_at",
                "id",
                "name",
                "nick",
                "activities",
                "bot",
                "raw_status",
                "status",
                "web_status",
                "desktop_status",
                "mobile_status",
                "roles",
                "top_role",
                "discriminator",
                "guild",
                "guild_permissions",
                "mention",
                "pending",
                "premium_since",
                "public_flags",
                "system",
                "voice",
            ]
        )

        @client.event
        async def on_ready():
            try:
                logging.info("Listening to guild info...")
                guild = client.get_guild(ins.access.get("guild_id"))

                # member is cached unexpectedly in different environment
                # To ensure we get the whole data, request member via web socket
                # https://discordpy.readthedocs.io/en/latest/api.html#discord.Guild.chunked
                # https://discordpy.readthedocs.io/en/latest/api.html#discord.Guild.chunk
                if not guild.chunked:
                    await guild.chunk()

                assert guild.member_count == len(
                    guild.members
                ), f"`member_count` `{guild.member_count}` is not equal to the number of members stored in the internal `members` cache `{len(guild.members)}`"

                for member in guild.members:
                    file_out_writer.writerow(
                        [
                            member.created_at,
                            member.joined_at,
                            member.id,
                            member.name,
                            member.nick,
                            member.activities,
                            member.bot,
                            member.raw_status,
                            member.status,
                            member.web_status,
                            member.desktop_status,
                            member.mobile_status,
                            member.roles,
                            member.top_role,
                            member.discriminator,
                            member.guild,
                            member.guild_permissions,
                            member.mention,
                            member.pending,
                            member.premium_since,
                            member.public_flags,
                            member.system,
                            member.voice,
                        ]
                    )

                file_out.seek(0)
                # file_out contains character is not Latin-1, convert to bytes
                file_out_bytes = BytesIO()
                file_out_bytes.write(file_out.getvalue().encode("utf-8"))
                file_out_bytes.seek(0)

                date_suffix = datetime.strftime(datetime.today(), "%Y%m%d")
                uploader = BigQueryDataUpload(
                    ins.service_account_json_path,
                    ins.dataset,
                    f"discord_user_info__{date_suffix}",
                )
                uploader.job_config.source_format = "CSV"
                uploader.load_file(file_out_bytes)

            except Exception as e:
                logging.error(e)
                errors.append(e)
            await client.close()

        client.run(ins.access.get("token"))
        if errors:
            raise ValueError(
                f"There was at least 1 ERROR during the event loop. List of errors {errors}"
            )
