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


# Columns to drop before loading.
# >50% null: always-empty, hardship plan, settlement, joint-application fields.
# ~37-47% null: sparse installment loan sub-fields.
# Redundant/low-value: investor split columns, free-text, masked identifiers,
#   post-origination FICO snapshots, and near-constant flag columns.
COLS_TO_DROP = [
    # Always null
    "member_id",
    # Hardship plan details (~99% null)
    "hardship_type",
    "hardship_reason",
    "hardship_status",
    "hardship_start_date",
    "hardship_end_date",
    "hardship_amount",
    "hardship_length",
    "hardship_dpd",
    "hardship_loan_status",
    "hardship_payoff_balance_amount",
    "hardship_last_payment_amount",
    "payment_plan_start_date",
    "deferral_term",
    "orig_projected_additional_accrued_interest",
    # Settlement details (~98% null)
    "settlement_status",
    "settlement_date",
    "settlement_amount",
    "settlement_percentage",
    "settlement_term",
    "debt_settlement_flag_date",
    # Joint application details (~94% null)
    "sec_app_fico_range_low",
    "sec_app_fico_range_high",
    "sec_app_earliest_cr_line",
    "sec_app_inq_last_6mths",
    "sec_app_mort_acc",
    "sec_app_open_acc",
    "sec_app_revol_util",
    "sec_app_open_act_il",
    "sec_app_num_rev_accts",
    "sec_app_chargeoff_within_12_mths",
    "sec_app_collections_12_mths_ex_med",
    "sec_app_mths_since_last_major_derog",
    "revol_bal_joint",
    "verification_status_joint",
    "annual_inc_joint",
    "dti_joint",
    # High-null optional fields (51-84% null)
    "mths_since_last_record",
    "mths_since_recent_bc_dlq",
    "mths_since_last_major_derog",
    "mths_since_recent_revol_delinq",
    "next_pymnt_d",
    "mths_since_last_delinq",
    # Sparse installment loan sub-fields (~37-47% null)
    "open_acc_6m",
    "open_act_il",
    "open_il_12m",
    "open_il_24m",
    "open_rv_12m",
    "open_rv_24m",
    "inq_fi",
    "total_cu_tl",
    "inq_last_12m",
    "total_bal_il",
    "il_util",
    "mths_since_rcnt_il",
    "all_util",
    "max_bal_bc",
    # Investor split columns (redundant with non-_inv counterparts)
    "funded_amnt_inv",
    "out_prncp_inv",
    "total_pymnt_inv",
    # Low-value identifiers and free text
    "url",
    "title",
    "emp_title",
    "policy_code",
    "pymnt_plan",
    # Post-origination FICO snapshots (keep origination fico_range_*)
    "last_fico_range_high",
    "last_fico_range_low",
    "last_credit_pull_d",
]


def load_accepted_loans(file_path: str, table_name: str, schema: str) -> None:
    engine = build_engine()
    df = pd.read_csv(file_path)
    logger.info("Normalising column names to lowercase with underscores...")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    before = len(df.columns)
    df = df.drop(columns=[c for c in COLS_TO_DROP if c in df.columns])
    logger.info("Dropped %d columns (%d remaining)", before - len(df.columns), len(df.columns))
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
    logger.info("Data loaded into staging table successfully.")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--file-path", required=True)
    parser.add_argument("--table-name", default="accepted_loans")
    parser.add_argument("--schema", default="staging")
    args = parser.parse_args()

    load_accepted_loans(
        file_path=args.file_path,
        table_name=args.table_name,
        schema=args.schema,
    )
