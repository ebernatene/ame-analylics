import os
import logging
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_POSTGRESQL_HOST = os.getenv("DB_POSTGRESQL_HOST")
DB_POSTGRESQL_PORT = os.getenv("DB_POSTGRESQL_PORT")
DB_POSTGRESQL_USERNAME = os.getenv("DB_POSTGRESQL_USERNAME")
DB_POSTGRESQL_PASSWORD = os.getenv("DB_POSTGRESQL_PASSWORD")
DB_POSTGRESQL_NAME = os.getenv("DB_POSTGRESQL_NAME")

QA_DB_POSTGRESQL_HOST = os.getenv("QA_DB_POSTGRESQL_HOST")
QA_DB_POSTGRESQL_PORT = os.getenv("QA_DB_POSTGRESQL_PORT")
QA_DB_POSTGRESQL_USERNAME = os.getenv("QA_DB_POSTGRESQL_USERNAME")
QA_DB_POSTGRESQL_PASSWORD = os.getenv("QA_DB_POSTGRESQL_PASSWORD")
QA_DB_POSTGRESQL_NAME = os.getenv("QA_DB_POSTGRESQL_NAME")

def connect_to_prod_db():
    url_db = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    logging.info(f"[{datetime.now()}] Connecting to DB {DB_POSTGRESQL_NAME}")
    try :
        engine = create_engine(
            url_db.format(
                user=DB_POSTGRESQL_USERNAME,
                password=DB_POSTGRESQL_PASSWORD,
                host=DB_POSTGRESQL_HOST,
                port=DB_POSTGRESQL_PORT,
                db_name=DB_POSTGRESQL_NAME,
            )
        )
        conn = engine.connect()
        logging.info(f"[{datetime.now()}] Connected to database {DB_POSTGRESQL_NAME}")
        return conn
    except Exception as e:
        logging.warning(f"[{datetime.now()}] ERROR connecting to DB {DB_POSTGRESQL_NAME}: {e}")
        raise

def connect_to_qa_db():
    url_db = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    logging.info(f"[{datetime.now()}] Connecting to DB {QA_DB_POSTGRESQL_NAME}")
    try :
        engine = create_engine(
            url_db.format(
                user=QA_DB_POSTGRESQL_USERNAME,
                password=QA_DB_POSTGRESQL_PASSWORD,
                host=QA_DB_POSTGRESQL_HOST,
                port=QA_DB_POSTGRESQL_PORT,
                db_name=QA_DB_POSTGRESQL_NAME,
            )
        )
        conn = engine.connect()
        logging.info(f"[{datetime.now()}] Connected to database {QA_DB_POSTGRESQL_NAME}")
        return conn
    except Exception as e:
        logging.warning(f"[{datetime.now()}] ERROR connecting to DB {QA_DB_POSTGRESQL_NAME}: {e}")
        raise