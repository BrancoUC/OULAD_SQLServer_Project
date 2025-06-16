
from pathlib import Path
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os


load_dotenv(Path(__file__).resolve().parent / ".env")

SERVER   = os.getenv("SQL_SERVER", "localhost")
DATABASE = os.getenv("SQL_DATABASE", "oulad_db")
DRIVER   = os.getenv("DRIVER", "ODBC Driver 17 for SQL Server").replace(" ", "+")

PARAMS = "TrustServerCertificate=yes"

CONN_STR = (
    f"mssql+pyodbc:///?odbc_connect={quote_plus(f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes')}"
)

def get_engine(fast=True):
    return create_engine(CONN_STR, fast_executemany=fast)
