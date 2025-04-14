try:
    import unzip_requirements
except ImportError:
    pass

import sqlite3
from notion_client import Client
import os
from typing import List, Dict
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import tempfile
import pickle

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

# If modifying these scopes, delete the file token.pickle.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

KG_TO_LBS = 2.2046226218

def workout_mapping(record, self):
    exercise_sql_id_to_notion_id = {
        page["properties"]["sql_id"]["number"]: page["id"]
        for page in self.notion_results["exercise"]
    }

    logger.info("Exercise sql id to notion id: %s", exercise_sql_id_to_notion_id[record[2]])
    return {
        "sql_id": {"number": record[0]},
        "Date": {"date": {"start": record[1]}},
        "Exercise": {
            "type": "relation",
            "relation": [
                {"id": exercise_sql_id_to_notion_id[record[2]]}
            ]
        },
        "Weight": {"number": round(record[3] * KG_TO_LBS, 1)},
        "Reps": {"number": record[4]},
    }

SQL_NOTION_MAPPING = {
    "bodyweight": {
        "query": "SELECT _id, date, value FROM MeasurementRecord WHERE measurement_id = 1 ORDER BY date DESC",
        "mapping": lambda record, self: {
            "sql_id": {"number": record[0]},
            "Date": {"date": {"start": record[1]}},
            "Value": {"number": record[2]},
        },
    },
    "exercise": {
        "query": "SELECT _id, name FROM exercise ORDER BY name ASC",
        "mapping": lambda record, self: {
            "sql_id": {"number": record[0]},
            "Name": {"title": [{"text": {"content": record[1]}}]},
        },
    },
    "workout": {
        "query": "SELECT _id, date, exercise_id, metric_weight, reps FROM training_log ORDER BY date DESC",
        "mapping": workout_mapping,
    },
}


class DatabaseSync:
    def __init__(
        self,
        notion_token: str,
        notion_bodyweight_database_id: str,
        notion_exercise_database_id: str,
        notion_workout_database_id: str,
    ):
        self.notion = Client(auth=notion_token)
        self.notion_database_ids = {
            "bodyweight": notion_bodyweight_database_id,
            "exercise": notion_exercise_database_id,
            "workout": notion_workout_database_id,
        }
        self.notion_results = {}
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "db.sqlite")
        self.get_latest_backup_file()

    def __del__(self):
        os.remove(self.db_path)
        os.rmdir(self.temp_dir)

    def _get_all_notion_records(self, database_id: str) -> List:
        """Get all existing exercise record IDs from Notion database."""
        start_cursor = None
        all_results = []
        while True:
            results = self.notion.databases.query(
                database_id=database_id, page_size=100, start_cursor=start_cursor
            )

            all_results.extend(results["results"])

            if results["has_more"]:
                start_cursor = results["next_cursor"]
            else:
                logger.debug("Found records %s", all_results)
                return all_results

    def get_latest_backup_file(self) -> str:
        """Get the latest FitNotes backup file from Google Drive."""
        creds = None
        # The file token.pickle stores the user's access and refresh tokens
        if os.path.exists("token.pickle"):
            with open("token.pickle", "rb") as token:
                creds = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if os.environ.get("IS_OFFLINE"):
                    flow = InstalledAppFlow.from_client_secrets_file(
                        "credentials.json", SCOPES
                    )
                    creds = flow.run_local_server(port=8080)
                else:
                    raise Exception("No valid credentials found")
            # Save the credentials for the next run
            if os.environ.get("IS_OFFLINE"):
                with open("token.pickle", "wb") as token:
                    pickle.dump(creds, token)

        service = build("drive", "v3", credentials=creds)

        # Search for FitNotes backup files
        results = (
            service.files()
            .list(
                q="name contains 'FitNotes_Backup_' and name contains '.fitnotes'",
                fields="files(id, name)",
                orderBy="name desc",
                pageSize=1,
            )
            .execute()
        )

        items = results.get("files", [])

        logger.info("Found files: %s", items)

        if not items:
            raise Exception("No FitNotes backup files found in Google Drive")

        latest_file = items[0]
        logger.info("Found latest backup file: %s", latest_file["name"])

        # Download the file
        request = service.files().get_media(fileId=latest_file["id"])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.info("Download %s%%", int(status.progress() * 100))

        # Save to temporary file
        with open(self.db_path, "wb") as f:
            f.write(fh.getvalue())

        logger.info("Downloaded db to: %s", self.db_path)

        return self.db_path

    def _sync_table(self, table_name: str):
        """Sync SQLite records to Notion database."""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get existing Notion record IDs
        notion_records = self._get_all_notion_records(self.notion_database_ids[table_name])
        self.notion_results[table_name] = notion_records
        existing_notion_ids = [
            page["properties"]["sql_id"]["number"] for page in notion_records
        ]

        logger.info("Found existing notion ids: %s", existing_notion_ids)

        # Get records from SQLite that don't have a notion_id
        cursor.execute(SQL_NOTION_MAPPING[table_name]["query"])

        records = cursor.fetchall()

        logger.debug("Found records: %s", records)

        for record in filter(lambda x: x[0] not in existing_notion_ids, records):
            sqlite_id = record[0]

            # Create new page in Notion
            new_page = {
                "parent": {
                    "database_id": self.notion_database_ids[table_name]
                },
                "properties": SQL_NOTION_MAPPING[table_name]["mapping"](record, self),
            }
            logger.info("New page: %s", new_page)

            try:
                response = self.notion.pages.create(**new_page)
                notion_id = response["id"]

                logger.info(
                    f"Successfully synced record {sqlite_id} to Notion: {notion_id}"
                )

            except Exception as e:
                logger.error(f"Failed to sync record {sqlite_id} to Notion: {str(e)}")

        conn.close()

    def sync_bodyweight(self):
        """Sync bodyweight records from SQLite to Notion database."""
        self._sync_table("bodyweight")

    def sync_exercises(self):
        """Sync exercise records from SQLite to Notion database."""
        self._sync_table("exercise")

    def sync_workouts(self):
        """Sync workout records from SQLite to Notion database."""
        self._sync_table("workout")


def run(event, context):
    """Main function to run the sync."""
    notion_token = os.getenv("NOTION_API_KEY")
    notion_bodyweight_database_id = os.getenv("NOTION_BODYWEIGHT_DATABASE_ID")
    notion_exercise_database_id = os.getenv("NOTION_EXERCISE_DATABASE_ID")
    notion_workout_database_id = os.getenv("NOTION_WORKOUT_DATABASE_ID")

    if (
        not notion_token
        or not notion_bodyweight_database_id
        or not notion_exercise_database_id
        or not notion_workout_database_id
    ):
        logger.error("Environment variables not set")
        return

    sync = DatabaseSync(
        notion_token,
        notion_bodyweight_database_id,
        notion_exercise_database_id,
        notion_workout_database_id,
    )
    sync.sync_bodyweight()
    sync.sync_exercises()
    sync.sync_workouts()


if __name__ == "__main__":
    run({}, {})
