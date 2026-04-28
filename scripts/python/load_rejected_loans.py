import logging
import os
from argparse import ArgumentParser
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_engine():
    host = os.environ.get("PGHOST", "")
    port = os.environ.get("PGPORT", "5432")
    database = os.environ.get("PGDATABASE", "")
    user = os.environ.get("PGUSER", "")
    password = os.environ.get("PGPASSWORD", "")

    if not all([host, port, database, user]):
        raise ValueError("Missing required PG* environment variables for database connection")

    dsn = (
        f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}"
    )
    return create_engine(dsn)


def load_rejected_loans(file_path: str, table_name: str, schema: str) -> None:
    engine = build_engine()
    df = pd.read_csv(file_path)
    logger.info("Normalising column names to lowercase with underscores...")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df["published_date"] = pd.to_datetime("today").date()

    df.to_sql(
        name=table_name,
        schema=schema,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )
    logger.info("Rejected loans loaded successfully.")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--file-path", required=True)
    parser.add_argument("--table-name", default="rejected_loans")
    parser.add_argument("--schema", default="staging")
    args = parser.parse_args()

    load_rejected_loans(
        file_path=args.file_path,
        table_name=args.table_name,
        schema=args.schema,
    )
