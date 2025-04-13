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
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

class DatabaseSync:
    def __init__(self, notion_token: str, notion_database_id: str):
        self.notion = Client(auth=notion_token)
        self.notion_database_id = notion_database_id
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "db.sqlite")
        
    def get_latest_backup_file(self) -> str:
        """Get the latest FitNotes backup file from Google Drive."""
        creds = None
        # The file token.pickle stores the user's access and refresh tokens
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if os.environ.get("IS_OFFLINE"):
                    flow = InstalledAppFlow.from_client_secrets_file(
                        'credentials.json', SCOPES)
                    creds = flow.run_local_server(port=8080)
                else:
                    raise Exception("No valid credentials found")
            # Save the credentials for the next run
            if os.environ.get("IS_OFFLINE"):    
                with open('token.pickle', 'wb') as token:
                    pickle.dump(creds, token)

        service = build('drive', 'v3', credentials=creds)
        
        # Search for FitNotes backup files
        results = service.files().list(
            q="name contains 'FitNotes_Backup_' and name contains '.fitnotes'",
            fields="files(id, name)",
            orderBy="name desc",
            pageSize=1
        ).execute()
        
        items = results.get('files', [])

        logger.info("Found files: %s", items)
        
        if not items:
            raise Exception("No FitNotes backup files found in Google Drive")
            
        latest_file = items[0]
        logger.info("Found latest backup file: %s", latest_file['name'])
        
        # Download the file
        request = service.files().get_media(fileId=latest_file['id'])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.info("Download %s%%", int(status.progress() * 100))
            
        # Save to temporary file
        with open(self.db_path, 'wb') as f:
            f.write(fh.getvalue())

        logger.info("Downloaded db to: %s", self.db_path)
            
        return self.db_path
    
    def get_existing_notion_records(self) -> List[str]:
        """Get all existing record IDs from Notion database."""
        results = self.notion.databases.query(
            database_id=self.notion_database_id,
            page_size=100
        )
        
        notion_ids = []
        for page in results["results"]:
            notion_ids.append(page["properties"]["sql_id"]["number"])
        
        return notion_ids
    
    def sync_to_notion(self):
        """Sync SQLite records to Notion database."""
        # Download the latest backup file
        self.get_latest_backup_file()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get existing Notion record IDs
        existing_notion_ids = self.get_existing_notion_records()

        logger.info("Found existing notion ids: %s", existing_notion_ids)

        # Get records from SQLite that don't have a notion_id
        cursor.execute("""
            SELECT _id, date, value
            FROM MeasurementRecord
            WHERE measurement_id = 1
            ORDER BY date DESC
        """)
        
        records = cursor.fetchall()

        logger.info("Found records: %s", records)
        
        for record in filter(lambda x: x[0] not in existing_notion_ids, records):
            sqlite_id, date, value = record
            
            # Create new page in Notion
            new_page = {
                "parent": {"database_id": self.notion_database_id},
                "sql_id": { 
                    "number": sqlite_id
                },
                "properties": {
                    "sql_id": {
                        "number": sqlite_id
                    },
                    "Date": {
                        "date": {
                            "start": date
                        }
                    },
                    "Value": {
                        "number": value
                    },
                }
            }
            
            try:
                response = self.notion.pages.create(**new_page)
                notion_id = response["id"]
                
                logger.info(f"Successfully synced record {sqlite_id} to Notion: {notion_id}")
                
            except Exception as e:
                logger.error(f"Failed to sync record {sqlite_id} to Notion: {str(e)}")
        
        conn.close()
        # Clean up temporary file
        os.remove(self.db_path)
        os.rmdir(self.temp_dir)

def run(event, context):
    """Main function to run the sync."""
    notion_token = os.getenv("NOTION_API_KEY")
    notion_database_id = os.getenv("NOTION_DATABASE_ID")
    
    if not notion_token or not notion_database_id:
        logger.error("NOTION_TOKEN or NOTION_DATABASE_ID environment variables not set")
        return
    
    sync = DatabaseSync(notion_token, notion_database_id)
    sync.sync_to_notion() 

if __name__ == "__main__":
    run({}, {})