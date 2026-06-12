# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle ➜ Postgres Table Migration
# MAGIC
# MAGIC This notebook migrates **400+ tables** from an Oracle database to a Postgres database by
# MAGIC reproducing the following flow for every table in the configured list:
# MAGIC
# MAGIC 1. **Dump** the entire table (DDL + data) from the source Oracle DB into an Oracle-flavoured `.sql` file.
# MAGIC 2. **Convert** that Oracle SQL file into a Postgres-flavoured `.sql` file (syntax conversion).
# MAGIC 3. **Load** the converted Postgres SQL file into the target Postgres DB table.
# MAGIC
# MAGIC **Architecture**
# MAGIC ```
# MAGIC  ┌────────────┐   1. dump    ┌──────────────┐   2. convert   ┌────────────────┐   3. load   ┌────────────┐
# MAGIC  │  Oracle DB │ ───────────► │ oracle/*.sql │ ─────────────► │ postgres/*.sql │ ──────────► │ Postgres DB│
# MAGIC  └────────────┘              └──────────────┘                └────────────────┘             └────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Notes**
# MAGIC - Connection details live in plain variables in the *Configuration* cell (no widgets).
# MAGIC - Replace the placeholder values / `TABLE_NAMES` list with your real values. Prefer pulling
# MAGIC   secrets from `dbutils.secrets` rather than hard-coding passwords (an example is shown, commented out).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install drivers
# MAGIC `python-oracledb` (thin mode, no Oracle client needed) for the source and `psycopg2` for the target.

# COMMAND ----------

# MAGIC %pip install oracledb psycopg2-binary
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration — connection details & table list (NO WIDGETS)

# COMMAND ----------

import os

# ----------------------------------------------------------------------------
# Oracle (SOURCE) connection details
#   Each value falls back to the literal below, but can be overridden by an
#   environment variable of the same name. That's what lets the Docker / Jupyter
#   stack point the notebook at the local containers without editing this cell.
# ----------------------------------------------------------------------------
ORACLE_HOST = os.getenv("ORACLE_HOST", "oracle-host.example.com")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "ORCLPDB1")   # use service name ...
ORACLE_SID = os.getenv("ORACLE_SID") or None                          # ... OR sid (leave one of them None)
ORACLE_USER = os.getenv("ORACLE_USER", "oracle_user")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "oracle_password")    # e.g. dbutils.secrets.get("scope", "oracle_pw")
ORACLE_SCHEMA = os.getenv("ORACLE_SCHEMA", "ORACLE_USER")            # schema that owns the tables (often == user, uppercase)

# ----------------------------------------------------------------------------
# Postgres (TARGET) connection details
# ----------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "postgres-host.example.com")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "target_db")
PG_USER = os.getenv("PG_USER", "postgres_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres_password")          # e.g. dbutils.secrets.get("scope", "pg_pw")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")                          # target schema in Postgres
PG_SSLMODE = os.getenv("PG_SSLMODE", "prefer")                        # disable | allow | prefer | require | verify-ca | verify-full

# ----------------------------------------------------------------------------
# Migration behaviour
# ----------------------------------------------------------------------------
WORK_DIR = os.getenv("WORK_DIR", "/dbfs/tmp/ora2pg")                 # where the .sql dump files are written
ORACLE_DUMP_DIR = os.path.join(WORK_DIR, "oracle")
POSTGRES_DUMP_DIR = os.path.join(WORK_DIR, "postgres")

BATCH_SIZE = 5_000                          # rows fetched per batch from Oracle
MAX_PARALLEL_TABLES = 8                      # how many tables to migrate concurrently (thread pool).
                                            # Each worker uses its own Oracle + Postgres connection,
                                            # so keep this <= the connection limits on BOTH databases.
                                            # Set to 1 for fully sequential migration.
# How to handle the target table on the Postgres side:
#   "recreate" — DROP (CASCADE) + CREATE from the Oracle DDL, then load. Runs ALL the structural
#                passes below (constraints/indexes/identity/sequences/FKs). Full migration. (default)
#   "truncate" — table must ALREADY EXIST. TRUNCATE it, then load data only. Skips CREATE and ALL
#                structural passes (your schema already has them). Use for "replace the data".
#   "append"   — table must ALREADY EXIST. Load data only (no drop/truncate/create). Skips ALL
#                structural passes. Use for "add data to what's there" (no dedup — may duplicate).
LOAD_MODE = "recreate"

CONTINUE_ON_ERROR = True                    # keep migrating remaining tables if one fails
KEEP_SQL_FILES = True                       # keep intermediate .sql files for auditing

# Constraint / index migration (second pass). Only applied when LOAD_MODE == "recreate".
MIGRATE_PK_UNIQUE_CHECK = True              # add PRIMARY KEY / UNIQUE / CHECK constraints
MIGRATE_INDEXES = True                      # re-create non-constraint indexes
MIGRATE_FOREIGN_KEYS = True                 # add FOREIGN KEYs (applied last, after all tables load)
MIGRATE_SEQUENCES = True                    # re-create standalone Oracle sequences in Postgres
MIGRATE_IDENTITY_COLUMNS = True             # convert Oracle IDENTITY columns to Postgres IDENTITY

# Load ordering.
RESPECT_LOAD_ORDER = False                  # True  = load tables exactly in TABLE_NAMES order
                                            # False = auto-sort by FK dependency (parents first)

# ----------------------------------------------------------------------------
# Example of sourcing secrets instead of hard-coding (recommended):
# ----------------------------------------------------------------------------
# ORACLE_PASSWORD = dbutils.secrets.get(scope="migration", key="oracle_password")
# PG_PASSWORD     = dbutils.secrets.get(scope="migration", key="pg_password")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table list (400+ tables)
# MAGIC Provide the table names as an array of strings. Two options are shown — pick one.

# COMMAND ----------

# Option A — explicit, hard-coded list of table names.
TABLE_NAMES = [
    "ACS_BACKUP_LIST",
    "ACS_EXTRACT",
    "ACS_EXTRACT_HISTORY",
    "ACS_EXTRACT_SUMM",
    "ACS_OP",
    "ACS_TRAN_TYPE",
    "ACS_TRANSACTION",
    "AGENCY_COMPENS_RATE",
    "AGENT",
    "AGENT_ASSIGNMENT",
    "AGENT_BANK",
    "AGENT_CDPA",
    "AGENT_CDPA_TRAN",
    "AGENT_COMMISSION",
    "AGENT_COMMISSION_STAGING",
    "AGENT_COMPENS_ELIG_AGENT_TYPE",
    "AGENT_COMPENS_ELIGIBILITY",
    "AGENT_COMPENS_ELIGIBILITY_NEW",
    "AGENT_CUMM_INCM",
    "AGENT_CUMM_INCM_DTL",
    "AGENT_CUMM_INCM_DTL_HIST",
    "AGENT_CUMM_TAX",
    "AGENT_CUMM_TAX_DTL",
    "AGENT_CUMM_TAX_DTL_HIST",
    "AGENT_DELTA",
    "AGENT_DISC",
    "AGENT_DIVISION",
    "AGENT_FORWARDED_BAL_CURR",
    "AGENT_FORWARDED_BAL_DTL",
    "AGENT_FORWARDED_BAL_DTL_IN",
    "AGENT_FORWARDED_BAL_SUMMARY",
    "AGENT_GA",
    "AGENT_GA_DETAIL",
    "AGENT_GA_DETAIL_NEW",
    "AGENT_HIERARCHY",
    "AGENT_HIERARCHY_APPOINT",
    "AGENT_HIERARCHY_CURRENT",
    "AGENT_HIERARCHY_MONTHLY",
    "AGENT_HIERARCHY_PREV",
    "AGENT_HIERARCHY_TEMP",
    "AGENT_HISTORICAL_HIERARCHY",
    "AGENT_JUM_AUDIT_TRAIL",
    "AGENT_LICENSE",
    "AGENT_LICENSE_NEW",
    "AGENT_MOVE_TYPE",
    "AGENT_MOVEMENT",
    "AGENT_MOVEMENT_AUM_FIX",
    "AGENT_MOVEMENT_DELETED",
    "AGENT_MOVEMENT_FORPROCESSING",
    "AGENT_MOVEMENT_PENDING",
    "AGENT_MOVEMENT_TEMP",
    "AGENT_MOVEMENT_TEMP_CUR",
    "AGENT_MOVEMENT_TOUPDATE",
    "AGENT_MOVEMENT_TREETEMP",
    "AGENT_MOVEMENT_VALIDATION",
    "AGENT_NAP",
    "AGENT_NAP_BACK",
    "AGENT_NEW",
    "AGENT_PAY_STAGE_IN",
    "AGENT_PAY_TRAN_CURR",
    "AGENT_PAY_TRAN_DTL",
    "AGENT_PAY_TRAN_DTL_HIST",
    "AGENT_PAY_TRAN_DTL_INVISTI",
    "AGENT_PAY_TRAN_SUMMARY",
    "AGENT_PAYOUT",
    "AGENT_PAYOUT_HELD",
    "AGENT_PERSONAL_INCOME",
    "AGENT_PREDISB",
    "AGENT_PREV",
    "AGENT_QTY_FACTOR",
    "AGENT_QTY_FACTOR_NEW",
    "AGENT_RECOMPENS_ELIGIBILITY",
    "AGENT_REGION",
    "AGENT_RELATIONSHIP",
    "AGENT_RELATIONSHIP_JUM",
    "AGENT_RELATIONSHIP_LIST",
    "AGENT_RELATIONSHIP_TEST",
    "AGENT_RELATIONSHIP_TYPE",
    "AGENT_SAVING_LVLUP",
    "AGENT_SAVING_PLAN",
    "AGENT_SAVING_PLAN_CAP",
    "AGENT_SAVING_PLAN_NEW",
    "AGENT_SAVING_PLAN_TRAN",
    "AGENT_SCHED_DEDUCTION",
    "AGENT_STATUS",
    "AGENT_SUSPENSION",
    "AGENT_SYO",
    "AGENT_TAGS",
    "AGENT_TAGS_STG",
    "AGENT_TAX_DTL",
    "AGENT_TAX_DTL_HIST",
    "AGENT_TAX_SUMMARY",
    "AGENT_TAX_SUMMARY_HIST",
    "AGENT_TAX_TAG",
    "AGENT_TEAM",
    "AGENT_TO_SYNC",
    "AGENT_TYPE",
    "AGENT_UPLINE_DATAMART",
    "AGENT_UPLINE_MISMATCHES",
    "AGENT_UPLINE_VALIDATION_LOG",
    "AGENT_WHTAX",
    "APC_EMP_REVIEWER",
    "API_CHANGE_TAB",
    "API_CHANGE_TAB_ANN",
    "APPROVAL_APC_AG_DTL",
    "APPROVAL_APC_EMP_DTL",
    "APPROVAL_BANK_DTL",
    "APPROVAL_REQUEST",
    "APPROVAL_TRAN_TYPE",
    "APPROVAL_TRANSACTION",
    "AREA",
    "AUM_LIST",
    "AUTO_CREDIT_SUMMARY",
    "AUTOCREDIT_ACCOUNT",
    "AUTOCREDIT_DISB",
    "AUTOCREDIT_DISB_DTL",
    "AUTOCREDIT_EMPLOYEE",
    "AUTOCREDIT_TRAN",
    "BANK_ACCOUNT_HISTORY",
    "BANK_ACCOUNT_STAGING",
    "BANK_PARTNERS",
    "BANK_TPD",
    "BOUNCED_CHECK",
    "BRANCH",
    "BRANCH_CEB",
    "BRANCH_IDSS",
    "BREAKAWAY_DEMOTIONS",
    "BREAKAWAY_PROMOTIONS",
    "CALENDAR",
    "CHECK_PAYMENT",
    "CLAWBACK",
    "CLIENT",
    "CLIENT_ADDRESS",
    "CLIENT_OTHER_INFO",
    "CLIENT_OTHER_INFO_HISTORY",
    "CLIENT_OTHER_INFO_STAGING",
    "COMBI_TAB",
    "COMM_DEPOSITED",
    "COMM_DEPOSITED_STAGING",
    "COMM_INCLUDE",
    "COMMCURSOR",
    "COMMISSION",
    "COMMISSION_DMTM",
    "COMMISSION_TEMP",
    "COMMISSION_TEMP_GMI",
    "COMMISSION_TPD",
    "COMPANY_SAVING_PLAN_CAP",
    "COMPENS_APE_HURDLE",
    "COMPENS_AUTO_DED_EXTRACT",
    "COMPENS_CRED",
    "COMPENS_CRED_INSTALLMENT",
    "COMPENS_CRED_MOVEMENT",
    "COMPENS_CRED_PAYT",
    "COMPENS_CRED_RECURR_MOVEMNT",
    "COMPENS_DA_REQ",
    "COMPENS_DED",
    "COMPENS_DED_INSTALLMENT",
    "COMPENS_DED_MOVEMENT",
    "COMPENS_DED_PAYT",
    "COMPENS_DED_RECURR_MOVEMNT",
    "COMPENS_MATRIX",
    "COMPENS_MATRIX_ERROR_LOG",
    "COMPENS_MATRIX_FORRO",
    "COMPENS_MATRIX_HIST",
    "COMPENS_MATRIX_IMPACT",
    "COMPENS_MATRIX_NOT_INSERTED",
    "COMPENS_MATRIX_SELLOUT",
    "COMPENS_MATRIX_TEMP",
    "COMPENS_MISC_CRED",
    "COMPENS_MISC_DED",
    "COMPENS_RATE_COMPPROD",
    "COMPENS_RATE_GAO",
    "COMPENS_RATE_IMDT_DLINE",
    "COMPENS_RATE_LEADPROD",
    "COMPENS_RATE_NUM_FACTOR",
    "COMPENS_RATE_PERSIST_GRP",
    "COMPENS_RATE_QTR_NAP",
    "COMPENS_RATE_RO",
    "COMPENS_RATE_TYPEPROD",
    "COMPENS_RATE_YRSERVICE",
    "COMPENS_RECURR_CRED",
    "COMPENS_RECURR_DED",
    "COMPENS_SCHEME",
    "COMPENS_TAB_AGENT",
    "COMPENS_TAB_AGENT_HISTORY",
    "COMPENS_TAB_ANNUAL_DTL",
    "COMPENS_TAB_ANNUAL_SUMM",
    "COMPENS_TAB_BMUM_NAP",
    "COMPENS_TAB_MP",
    "COMPENS_TAB_NAP",
    "COMPENS_TAB_NAP_ANN",
    "COMPENS_TAB_QTOTAL",
    "COMPENS_TAB_RATES",
    "COMPENS_TAB_RECRUIT",
    "COMPENS_TAB_SPARAM",
    "COMPENS_TYPE",
    "COMPENS_TYPE_AGENT_TYPE",
    "COMPENS_TYPE_CATEGORY",
    "COMPENS_TYPE_CLASS",
    "COMPENS_TYPE_DIST_CHANNEL",
    "COMPENS_TYPE_GRP",
    "COMPENS_TYPE_SCHEME",
    "COMPENS_YTDNAP_HURDLE",
    "CONTRACT_TYPE",
    "CONTRACT_TYPE_ELIG",
    "CUT_OFF_SCHED",
    "CUT_OFF_SCHED_BCKP",
    "DELETE_AGENT_MOVEMENT",
    "DELIMITED_FILE_LIST",
    "DELIMITED_FILE_VALIDATION",
    "DEPARTMENT",
    "DEPT_SERVICE",
    "DISBURSING_BANK",
    "DISTRIBUTION_CHANNEL",
    "DMPATPNS",
    "DOMAIN",
    "EXTRACT_LIST",
    "EXTRACT_LIST_TEST",
    "EXTRACT_PROCESS_ERROR",
    "FEATURE_CONTROL",
    "FIN_MANUAL_PAYMENT",
    "FIN_PAYOUT_SUMMARY",
    "FROM_SIRWILL",
    "GET_MANPOWER_REQUEST",
    "GL_ACCT",
    "GL_TRAN_DTL",
    "GL_TRAN_DTL_HIST",
    "GL_TRAN_DTL_POLREP",
    "GL_TRAN_DTL_RPT",
    "GL_TRAN_DTL_TERMINATED",
    "GL_TRAN_DTL_TPD",
    "GL_TRAN_SUMMARY",
    "GL_TRAN_SUMMARY_BKP",
    "GL_TRAN_SUMMARY_POLREP",
    "GMI_TEST",
    "GROSS_INCOME",
    "GROSS_YTD",
    "GROUP_SCHED",
    "HASH_SUMMARY",
    "HASH_SUMMARY_HISTORY",
    "HASH_SUMMARY_OPTIMIZATION",
    "HISTORICAL_DATA",
    "HYPERLINK",
    "HYPERLINK_HIERARCHY",
    "INORGANI_ORC",
    "INORGANIC_ADJUSTMENTS",
    "INORGANIC_DISBURSE",
    "INORGANIC_ENROLLEES",
    "INORGANIC_MP_DTL",
    "INORGANIC_NAP_DTL",
    "INORGANIC_PACKAGES",
    "INORGANIC_QUALIFIERS",
    "INORGANIC_TARGETS",
    "JAVA",
    "JAVA_CLASS",
    "KILL_SWITCH",
    "LETTER_OF_INSTRUCTION",
    "LVL_UP_QUALIFIER",
    "MANPOWER_OF",
    "MANPOWER_SOURCE",
    "MANUAL_PAYMENT",
    "MANUAL_PAYMENT_BACK",
    "MD_APPLICATIONS",
    "MIGRATION_AGENT_TIN_NO",
    "MIGRATION_FINAL_TAB",
    "MIGRATION_PAYOUT_DTL",
    "MIGRATION_PAYOUT_SUMMARY",
    "MIGRATION_PREMIUM_RCVD",
    "MO_COMPENS_MATRIX_ORC",
    "MODEL_TYPE",
    "NEW_AGENTS",
    "NEW_AGENTS_TEST",
    "NOMANAGER",
    "ORC_FYO",
    "ORPHAN_SYO",
    "PAYOUT_DTL",
    "PAYOUT_SUMMARY",
    "PAYROLL_SUMMARY",
    "PERSISTENCY",
    "PLAN",
    "PO",
    "PO_REL",
    "POLICY",
    "POLICY_BREAKAWAY",
    "POLICY_BREAKAWAY_TEST",
    "POLICY_DELIVERY_RECEIPT",
    "POLICY_EXTENSION",
    "POLICY_REPLACEMENT",
    "POLICY_REPLACEMENT_BCK",
    "POLICY_REPLACEMENT_REF",
    "POLICY_RIDER",
    "PREMIUM_RCVD",
    "PROCESS",
    "PROCESS_DEPENDENCY",
    "PROCESS_DURATION",
    "PROCESS_ERROR_LOG",
    "PROCESS_LOG",
    "PRODFIX_AGENT_SCHED_DEDUCTION",
    "PRODUCTION",
    "PRODUCTION_BACKUP",
    "PRODUCTION_SUMMARY",
    "PRODUCTION_UNIT",
    "PRUACE_ADJUSTMENTS",
    "PRUACE_CC_DTL",
    "PRUACE_DISBURSE",
    "PRUACE_DISBURSE_BK",
    "PRUACE_ENROLLEES",
    "PRUACE_NAP_DTL",
    "PRUACE_PACKAGES",
    "PRUACE_QUALIFIERS",
    "PRUACE_TARGETS",
    "QUALITY_FACTOR",
    "REF_SCHED",
    "REPORT_UI",
    "REVERSAL_CODE",
    "RIMON_AGENT",
    "RIMON_AGENT_DOWNLINES",
    "RIMON_AGENT_RELATIONSHIP",
    "RIMON_AGENT_TAGS",
    "RIMON_FEATURE_CONTROL",
    "RIMON_HIERARCHY_TAB",
    "RIMON_SOA_SUMMARY",
    "RMA_CONTRACT_TYPE",
    "RMA_NEW_AGENTS",
    "RMA_TAG_TYPE",
    "RO_TEMP_TABLE",
    "ROLE",
    "ROLE_SERVICE",
    "SAVINGSFUND_STG",
    "SBS_BASE_DATA",
    "SBS_BASE_DATA_KOA",
    "SELLOUT_AGREEMENTS",
    "SELLOUT_ENROLLEES",
    "SERVICE",
    "SIGNATORY",
    "SOA_BALANCE_FORWARDED",
    "SOA_BALANCE_FORWARDED_BK",
    "SOA_BONUS_DTL",
    "SOA_BONUS_DTL_BK",
    "SOA_COMMISSION_DTL",
    "SOA_COMMISSION_DTL_BK",
    "SOA_CRED_TO_BPI",
    "SOA_CRED_TO_BPI_BK",
    "SOA_GA_OVERRIDE_DTL",
    "SOA_GA_OVERRIDE_DTL_BK",
    "SOA_HOLD_BONUS_DTL",
    "SOA_HOLD_BONUS_DTL_BK",
    "SOA_HOLD_COMMISSION_DTL",
    "SOA_HOLD_COMMISSION_DTL_BK",
    "SOA_HOLD_GA_OVERRIDE_DTL",
    "SOA_HOLD_GA_OVERRIDE_DTL_BK",
    "SOA_HOLD_OVERRIDE_DTL",
    "SOA_HOLD_OVERRIDE_DTL_BK",
    "SOA_HOLD_OVERRIDE_SELLOUT",
    "SOA_HOLD_OVERRIDE_XSELLOUT",
    "SOA_LIST",
    "SOA_LIST_BK",
    "SOA_LIST_ORIG",
    "SOA_LISTBACKUP",
    "SOA_MISC_ADJUSTMENTS",
    "SOA_MISC_ADJUSTMENTS_BK",
    "SOA_MISC_CR_DTL",
    "SOA_MISC_CR_DTL_BK",
    "SOA_MISC_DR_DTL",
    "SOA_MISC_DR_DTL_BK",
    "SOA_MULTIPLE_SUSPENSION",
    "SOA_MULTIPLE_SUSPENSION_BK",
    "SOA_NOT_DEDUCTED",
    "SOA_NOT_DEDUCTED_BK",
    "SOA_OVERRIDE_DTL",
    "SOA_OVERRIDE_DTL_BK",
    "SOA_OVERRIDE_SELLOUT",
    "SOA_OVERRIDE_XSELLOUT",
    "SOA_POLICY_REVERSAL",
    "SOA_POLICY_REVERSAL_BK",
    "SOA_POLREP",
    "SOA_POLREP_BK",
    "SOA_SUMMARY",
    "SOA_TAB_PARAM",
    "SOA_TPD_POLREP",
    "SOA_TPD_POLREPREF",
    "SOA_TPD_SUMM",
    "SOA_YTD",
    "SOA_YTD_BK",
    "SPLIT_COMMISSION",
    "SUMMARY_REPORTS_CURRENT",
    "SUMMARY_REPORTS_SAVINGS",
    "SUPER_EXTRACT_DTL",
    "SUPER_EXTRACT_SUMM",
    "SUSPENDED_PAYOUT",
    "SUSPENSION_MONITORING",
    "SYS_AUDIT_TRAIL",
    "SYS_ERROR",
    "SYS_PARAMETER",
    "SYS_REF",
    "SYS_REF_TABLE",
    "SYS_USER",
    "TAB_CANCELLATIONS",
    "TAB_REVERSAL",
    "TAG_TYPE",
    "TAX",
    "TAX_UPGRADE",
    "TEMP_AGENT_CODEERROR",
    "TEMP_ERROR_FINDER",
    "TEMP_JUMERROR",
    "TEMP_LR_HIERARCHY",
    "TEMP_TABLE",
    "TEMP_TRANNO",
    "TEST",
    "TEST_AGENT_TAGS",
    "TEST_AGENTS",
    "TMP_AGU_CALLERS",
    "TMP_AGU_RUNTIME_AUDIT",
    "TMP_UPLINE_COMPARE",
    "TOAD_PLAN_SQL",
    "TOAD_PLAN_TABLE",
    "TRANSACTION",
    "TRANSACTION_OLD",
    "UPLINE_TEST_CASES",
    "UPLINE_TEST_DIFFS",
    "UPLINE_TEST_RESULTS",
    "UPLINE_TEST_RESULTS_MOD",
    "UPLINE_TEST_RESULTS_ORIG",
    "USER_OBJECTS_RIMON",
    "X_TABLE_ERROR_LOG",
    "X_TABLE_ERROR_SUMMARY_LOG",
    "XAGENTASSGN_HIST",
    "XAGENTASSGN_STAGING",
    "XAGENTDEDUCTION_HIST",
    "XAGENTDEDUCTION_STAGING",
    "XAGENTPERSISTENCY_HIST",
    "XAGENTPERSISTENCY_STAGING",
    "XAPTD",
    "XBOUNCHEDCHK_HIST",
    "XBOUNCHEDCHK_STAGING",
    "XCHEQUERCPT_HIST",
    "XCHEQUERCPT_STAGING",
    "XCLIENT_HIST",
    "XCLIENT_STAGING",
    "XCOMMISSION_HIST",
    "XCOMMISSION_STAGING",
    "XCONTRACT_HIST",
    "XCONTRACT_STAGING",
    "XCONTRACTEXT_HIST",
    "XCONTRACTEXT_STAGING",
    "XCONTRACTTPD_HIST",
    "XCONTRACTTPD_STAGING",
    "XCOVERRIDER_HIST",
    "XCOVERRIDER_STAGING",
    "XGLCODES_HIST",
    "XGLCODES_STAGING",
    "XGROUPPERSISTENCY_HIST",
    "XGROUPPERSISTENCY_STAGING",
    "XMP",
    "XPOLREP_HIST",
    "XPOLREP_STAGING",
    "XPOLREPREF_HIST",
    "XPOLREPREF_STAGING",
    "XPOLREPREFTPD_HIST",
    "XPOLREPREFTPD_STAGING",
    "XPOLREPTPD_HIST",
    "XPOLREPTPD_STAGING",
    "XPOLRIDER_HIST",
    "XPOLRIDER_STAGING",
    "XPREMIUMRCVD_HIST",
    "XPREMIUMRCVD_STAGING",
    "XSAVINGPLAN_HIST",
    "XSAVINGPLAN_STAGING",
    "XSPLITCOMM_HIST",
    "XSPLITCOMM_STAGING",
    "YEAR_REFERENCE",
]

# Option B — discover every table in the Oracle schema automatically.
# Set DISCOVER_TABLES = True to ignore the list above and pull all tables in ORACLE_SCHEMA.
DISCOVER_TABLES = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Database connection helpers

# COMMAND ----------

import oracledb
import psycopg2

# Fetch every Oracle NUMBER as an exact decimal.Decimal instead of a Python float.
# oracledb defaults NUMBER -> float, which loses precision: a NUMBER(38,38) value
# comes back rounded (e.g. to -1.0), which then OVERFLOWS the target NUMERIC(38,38)
# and aborts the whole-table COPY ("numeric field overflow"). Decimal preserves the
# full precision/scale, and _csv_value stringifies Decimal losslessly.
oracledb.defaults.fetch_decimals = True


def get_oracle_connection():
    """Open a connection to the source Oracle database (python-oracledb thin mode)."""
    if ORACLE_SERVICE_NAME:
        dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE_NAME)
    elif ORACLE_SID:
        dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, sid=ORACLE_SID)
    else:
        raise ValueError("Either ORACLE_SERVICE_NAME or ORACLE_SID must be set.")
    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)
    # NOTE: we deliberately do NOT pin the session time zone. On oracledb thin mode
    # the dump is session-TZ-independent: TIMESTAMP WITH LOCAL TIME ZONE fetches the
    # stored UTC instant (naive), and plain TIMESTAMP / DATE are unaffected — verified
    # byte-identical with and without a session-TZ pin. (TIMESTAMP WITH TIME ZONE loses
    # its stored offset at fetch regardless of session zone — a documented driver
    # limitation; see run_adversarial_test.py.) An ALTER SESSION here would be an inert
    # no-op and the only connection-setup statement that could fail (ORA-01031) under a
    # PDB lockdown profile, so we omit it.
    return conn


def get_postgres_connection():
    """Open a connection to the target Postgres database."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode=PG_SSLMODE,
    )


# Make sure the working directories exist.
os.makedirs(ORACLE_DUMP_DIR, exist_ok=True)
os.makedirs(POSTGRES_DUMP_DIR, exist_ok=True)
print(f"Oracle dump dir   : {ORACLE_DUMP_DIR}")
print(f"Postgres dump dir : {POSTGRES_DUMP_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. STEP 1 — Dump an Oracle table to a DDL `.sql` file + a COPY-ready CSV
# MAGIC
# MAGIC For speed, data is written as a **CSV** (Postgres `COPY` format) rather than per-row
# MAGIC `INSERT`s — `COPY` is dramatically faster on large tables. The `CREATE TABLE` DDL is
# MAGIC written to a separate `.sql` file (built from `all_tab_columns` for accurate Oracle types)
# MAGIC and is the only thing the Step 2 converter has to touch. Values are formatted directly into
# MAGIC Postgres-friendly text in Python, so the data needs no SQL-literal conversion at all.

# COMMAND ----------

import csv
import datetime
import decimal


import hashlib


def _short_ident(name: str) -> str:
    """
    Shorten an identifier that would exceed Postgres' 63-byte limit, deterministically.

    Postgres SILENTLY truncates any identifier longer than 63 bytes (NAMEDATALEN-1).
    Two long Oracle names that share their first 63 bytes would collide on the
    Postgres side (DuplicateColumn / DuplicateObject), and a column name truncated in
    CREATE TABLE but referenced un-truncated elsewhere would mismatch. To stay
    consistent we apply ONE canonical shortener everywhere a name is emitted INTO
    Postgres (column names in CREATE TABLE, the COPY column list, constraint / index /
    FK names, and column lists inside constraints).

    Strategy: if the name already fits in 63 bytes, return it UNCHANGED (so every
    real-world <63-char identifier — including the existing test fixtures — is
    untouched). Otherwise keep the first 57 bytes and append '_' + a 5-hex-char digest
    of the FULL original name (total <= 63 bytes), which is stable for a given input
    and distinguishes names that share a long common prefix.

    This is applied ONLY to the Postgres-side identifier. The Oracle-side dictionary
    queries keep the real Oracle identifiers (they must, to read the source).
    """
    encoded = name.encode("utf-8")
    if len(encoded) <= 63:
        return name
    digest = hashlib.blake2b(encoded, digest_size=8).hexdigest()[:5]
    # Truncate the prefix on a byte boundary, then back off if we split a multibyte
    # char, so the result is always valid UTF-8 and <= 63 bytes.
    prefix = encoded[:57]
    while True:
        try:
            prefix_str = prefix.decode("utf-8")
            break
        except UnicodeDecodeError:
            prefix = prefix[:-1]
    return f"{prefix_str}_{digest}"


def _oracle_quote_ident(name: str) -> str:
    """Quote an Oracle identifier."""
    return '"' + name.replace('"', '""') + '"'


def _oracle_output_type_handler(cursor, name, default_type, size, precision, scale):
    """
    Fetch LOBs as plain values instead of locators: CLOB/NCLOB -> str, BLOB -> bytes.
    This avoids a per-row round trip per LOB and is much faster for dumping.
    """
    if default_type in (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB):
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if default_type == oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
    return None


def _oracle_column_type(data_type, length, precision, scale, char_length=None) -> str:
    """Build an Oracle column type string from all_tab_columns metadata.

    ``char_length`` is the column's length in *characters* (all_tab_columns.char_length).
    For character types it is preferred over ``length`` (data_length, in BYTES): on an
    AL32UTF8 database a ``VARCHAR2(100 CHAR)`` column reports data_length=400 but
    char_length=100, so using data_length would quadruple the declared size. For
    byte-semantics columns char_length equals the declared length, so preferring it
    when present is correct in both cases. Falls back to ``length`` if char_length is
    0/NULL. RAW deliberately keeps using ``length`` (it is genuinely byte-sized).
    """
    dt = (data_type or "").upper()
    if dt in ("VARCHAR2", "VARCHAR", "NVARCHAR2", "CHAR", "NCHAR"):
        char_len = char_length if (char_length and int(char_length) > 0) else length
        return f"{dt}({char_len or 4000})"
    if dt == "NUMBER":
        # Oracle NUMBER scale can be negative (rounding to the left of the decimal
        # point) or the sentinel -127 (the "floating NUMBER" subtype with binary
        # precision and no fixed scale). Postgres NUMERIC rejects a negative scale,
        # so we never emit one.
        if scale is not None and int(scale) == -127:
            # Floating NUMBER subtype: emit bare NUMBER -> unbounded NUMERIC, which
            # preserves all digits rather than picking an arbitrary fixed scale.
            return "NUMBER"
        if scale is not None and int(scale) < 0:
            # Real negative scale (e.g. NUMBER(5,-2)): Postgres can't store negative
            # scale. Drop to scale 0 (Oracle's left-of-point rounding is a storage
            # detail, not representable in NUMERIC's typemod). Bare NUMBER if no
            # precision is known.
            return f"NUMBER({precision})" if precision else "NUMBER"
        if precision:
            return f"NUMBER({precision},{int(scale or 0)})"
        if scale is not None and int(scale) >= 0:
            # NUMBER(*,s): precision is NULL but a scale is declared. Use Oracle's
            # maximum precision (38) so the scale is preserved.
            return f"NUMBER(38,{int(scale)})"
        return "NUMBER"
    if dt.startswith("TIMESTAMP"):
        return dt  # preserve precision / WITH TIME ZONE, the converter normalises it
    if dt in ("ROWID", "UROWID"):
        # Oracle physical/logical row addresses. Postgres has no equivalent type
        # (and "rowid" is not a real Postgres type), so the dumped value is just
        # an opaque string. Map to TEXT; the _TYPE_RULES ROWID/UROWID->TEXT rules
        # are a belt-and-suspenders for any other code path.
        return "TEXT"
    if dt in ("DATE", "CLOB", "NCLOB", "BLOB", "LONG", "FLOAT", "BINARY_FLOAT",
              "BINARY_DOUBLE"):
        return dt
    if dt == "RAW":
        return f"RAW({length or 2000})"
    # Fallback for anything unusual.
    return "VARCHAR2(4000)"


def _build_create_table_ddl(ora_conn, table_name: str) -> str:
    """Build an Oracle CREATE TABLE statement from the data dictionary (accurate types)."""
    owner = ORACLE_SCHEMA.upper()
    cur = ora_conn.cursor()

    # Identity columns carry a system sequence as their "default"; that is handled
    # by the identity pass, so we must NOT emit it as a column DEFAULT here.
    cur.execute(
        "SELECT column_name FROM all_tab_identity_cols "
        "WHERE owner = :owner AND table_name = :tname",
        owner=owner, tname=table_name,
    )
    identity_cols = {r[0] for r in cur.fetchall()}

    # Use ALL_TAB_COLS (not ALL_TAB_COLUMNS): only ALL_TAB_COLS exposes
    # VIRTUAL_COLUMN / HIDDEN_COLUMN. We filter hidden_column='NO' so the result
    # matches what ALL_TAB_COLUMNS / the data dump's SELECT * actually return
    # (ALL_TAB_COLS additionally lists system-generated hidden columns — e.g. the
    # backing column of a function-based index — which must NOT appear in CREATE
    # TABLE or the COPY column list would misalign).
    cur.execute(
        """
        SELECT column_name, data_type, data_length, data_precision, data_scale,
               nullable, data_default, char_length, virtual_column
        FROM all_tab_cols
        WHERE owner = :owner AND table_name = :tname
          AND hidden_column = 'NO'
        ORDER BY column_id
        """,
        owner=owner, tname=table_name,
    )
    cols = cur.fetchall()
    cur.close()
    if not cols:
        raise ValueError(f"No columns found for {ORACLE_SCHEMA}.{table_name}")

    col_defs = []
    for (col_name, data_type, length, precision, scale, nullable, data_default,
         char_length, virtual_column) in cols:
        col_type = _oracle_column_type(data_type, length, precision, scale, char_length)
        not_null = "" if nullable == "Y" else " NOT NULL"

        is_virtual = (virtual_column or "").upper() == "YES"

        default_clause = ""
        if is_virtual:
            # Oracle virtual (generated) columns carry a data_default that is an
            # EXPRESSION referencing OTHER columns (e.g. "QTY"*"PRICE"). Postgres
            # forbids column references in a plain DEFAULT, so emitting it would make
            # CREATE TABLE fail — and since DROP ... CASCADE already ran, the table
            # would be LOST. We also cannot emit a Postgres GENERATED ... STORED
            # column: the data dump does SELECT * (which already includes the
            # column's computed values) and COPY cannot write into a GENERATED
            # column, which would break the load. So we materialise it as a PLAIN
            # column holding the Oracle-computed snapshot, with no DEFAULT.
            print(f"  [virtual] {table_name}.{col_name}: Oracle virtual column "
                  f"materialised as a plain column (snapshot of values, no longer "
                  f"auto-computed).")
        elif col_name not in identity_cols and data_default is not None:
            # data_default is Oracle source text (LONG); trim and convert the
            # expression (SYSTIMESTAMP -> CURRENT_TIMESTAMP, NVL -> COALESCE, ...).
            raw_default = str(data_default).strip()
            if raw_default and raw_default.upper() != "NULL":
                default_clause = f" DEFAULT {convert_oracle_sql_text(raw_default).strip()}"

        # Emit the Postgres-side (possibly shortened) column name so it MATCHES the
        # COPY column list and the constraint/index column lists, all of which also
        # go through _short_ident. (No-op for names <= 63 bytes.)
        col_defs.append(
            f"    {_oracle_quote_ident(_short_ident(col_name))} "
            f"{col_type}{default_clause}{not_null}"
        )

    return (
        f"-- Oracle DDL for {ORACLE_SCHEMA}.{table_name}\n"
        f"CREATE TABLE {_oracle_quote_ident(_short_ident(table_name))} (\n"
        + ",\n".join(col_defs)
        + "\n);\n"
    )


def _csv_value(value):
    """Format a Python value as Postgres COPY-CSV text. None -> '' (loaded as NULL)."""
    if value is None:
        return None  # csv.writer emits an empty field; COPY ... NULL '' reads it as NULL
    if isinstance(value, oracledb.LOB):
        value = value.read()          # CLOB -> str, BLOB -> bytes; then fall through
        return _csv_value(value)
    if isinstance(value, datetime.datetime):
        # Build the year/month/day explicitly: glibc strftime does NOT zero-pad
        # years < 1000 ("%Y" on year 1 -> "1"), and Postgres then reads "1-01-01"
        # as year 2001 (its 2-digit-year heuristic) -> SILENT corruption. Format
        # the year as 4 digits ourselves so years 1..999 round-trip exactly.
        base = (f"{value.year:04d}-{value.month:02d}-{value.day:02d} "
                f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
                f".{value.microsecond:06d}")
        if value.tzinfo is not None:
            # Preserve the UTC offset for TIMESTAMP WITH (LOCAL) TIME ZONE columns.
            return base + value.strftime("%z")
        return base
    if isinstance(value, datetime.date):
        # Same year zero-pad fix as above (date has no time component).
        return f"{value.year:04d}-{value.month:02d}-{value.day:02d}"
    if isinstance(value, (bytes, bytearray)):
        return "\\x" + value.hex()           # Postgres bytea hex input (literal in CSV mode)
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, str):
        # Postgres text/varchar CANNOT store a NUL byte (\x00); a CHR(0) inside a
        # CLOB/VARCHAR2 value aborts the whole-table COPY with
        # "invalid byte sequence for encoding ... 0x00". Oracle does allow embedded
        # NULs in text, so strip them here (silently — a per-row warning would be
        # far too noisy on large tables). This also covers CLOB values, which reach
        # here as str after the LOB .read() branch above recurses.
        if "\x00" in value:
            value = value.replace("\x00", "")
        return value
    return value                              # int / float / Decimal — csv.writer stringifies


def dump_oracle_table(ora_conn, table_name: str):
    """
    STEP 1: Dump a single Oracle table to (DDL .sql, data .csv).
    Returns (ddl_path, csv_path). The CSV's first row is the column header.
    """
    ddl_path = os.path.join(ORACLE_DUMP_DIR, f"{table_name}.sql")
    csv_path = os.path.join(ORACLE_DUMP_DIR, f"{table_name}.csv")
    qualified = f"{_oracle_quote_ident(ORACLE_SCHEMA)}.{_oracle_quote_ident(table_name)}"

    # DDL from the dictionary (decoupled from the data cursor's LOB handler).
    with open(ddl_path, "w", encoding="utf-8") as f:
        f.write(_build_create_table_ddl(ora_conn, table_name))

    # Data as COPY-ready CSV.
    cursor = ora_conn.cursor()
    cursor.arraysize = BATCH_SIZE
    cursor.outputtypehandler = _oracle_output_type_handler
    cursor.execute(f"SELECT * FROM {qualified}")
    col_names = [c[0] for c in cursor.description]

    row_count = 0
    with open(csv_path, "w", encoding="utf-8", newline="") as cf:
        writer = csv.writer(cf, lineterminator="\n")
        writer.writerow(col_names)                      # header row
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            writer.writerows([_csv_value(v) for v in row] for row in rows)
            row_count += len(rows)
    cursor.close()

    print(f"  [dump]    {table_name}: {row_count} rows -> {csv_path}")
    return ddl_path, csv_path


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. STEP 2 — Convert Oracle `.sql` to Postgres `.sql` (syntax conversion)
# MAGIC
# MAGIC A line/regex based converter covering the most common Oracle ➜ Postgres differences:
# MAGIC data types, function names, sequence/DUAL idioms, identifier quoting and date literals.
# MAGIC Extend `_TYPE_RULES` / `_FUNC_RULES` for any project-specific syntax you hit.
# MAGIC
# MAGIC > **Scope note:** This is a **syntax-only, regex-based** converter — not a SQL parser/transpiler.
# MAGIC > It is designed to convert the **table data dumps generated by Step 1 of this notebook**
# MAGIC > (structured `CREATE TABLE` DDL + `INSERT` statements with predictable literals), which is the
# MAGIC > only input it will ever receive here. It is **not** intended for arbitrary hand-written Oracle
# MAGIC > SQL or PL/SQL (e.g. `CONNECT BY`, `(+)` joins, `DECODE`, `MERGE`, procedures/packages/triggers).
# MAGIC > For those, use a dedicated transpiler such as `ora2pg`.

# COMMAND ----------

import re

# NOTE: Syntax-only converter. Input is always the Step 1 table-data dump
# (CREATE TABLE + INSERTs), never free-form Oracle SQL / PL/SQL. See the scope
# note above before extending these rules for any other use.

# Data-type rewrites (applied to DDL). Order matters — longest/most specific first.
_TYPE_RULES = [
    (re.compile(r"\bVARCHAR2\s*\(\s*(\d+)\s*(?:CHAR|BYTE)?\s*\)", re.IGNORECASE), r"VARCHAR(\1)"),
    (re.compile(r"\bVARCHAR2\b", re.IGNORECASE), "VARCHAR"),
    (re.compile(r"\bNVARCHAR2\s*\(\s*(\d+)\s*\)", re.IGNORECASE), r"VARCHAR(\1)"),
    (re.compile(r"\bNCHAR\b", re.IGNORECASE), "CHAR"),
    (re.compile(r"\bCLOB\b", re.IGNORECASE), "TEXT"),
    (re.compile(r"\bNCLOB\b", re.IGNORECASE), "TEXT"),
    (re.compile(r"\bBLOB\b", re.IGNORECASE), "BYTEA"),
    (re.compile(r"\bLONG\s+RAW\b", re.IGNORECASE), "BYTEA"),
    (re.compile(r"\bRAW\s*\(\s*\d+\s*\)", re.IGNORECASE), "BYTEA"),
    # ROWID / UROWID have no Postgres type (a bare "rowid" would fail with
    # 'type "rowid" does not exist'). The dumped value is an opaque string.
    (re.compile(r"\bUROWID\b", re.IGNORECASE), "TEXT"),
    (re.compile(r"\bROWID\b", re.IGNORECASE), "TEXT"),
    (re.compile(r"\bLONG\b", re.IGNORECASE), "TEXT"),
    # NUMBER(p,0) -> integer-ish, NUMBER(p,s) -> NUMERIC(p,s), bare NUMBER -> NUMERIC
    (re.compile(r"\bNUMBER\s*\(\s*(\d+)\s*,\s*0\s*\)", re.IGNORECASE), r"NUMERIC(\1)"),
    (re.compile(r"\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.IGNORECASE), r"NUMERIC(\1,\2)"),
    (re.compile(r"\bNUMBER\b", re.IGNORECASE), "NUMERIC"),
    (re.compile(r"\bBINARY_DOUBLE\b", re.IGNORECASE), "DOUBLE PRECISION"),
    (re.compile(r"\bBINARY_FLOAT\b", re.IGNORECASE), "REAL"),
    (re.compile(r"\bFLOAT\b", re.IGNORECASE), "DOUBLE PRECISION"),
    # Postgres has no "WITH LOCAL TIME ZONE"; it's a timestamptz -> normalise it.
    (re.compile(r"\bWITH\s+LOCAL\s+TIME\s+ZONE\b", re.IGNORECASE), "WITH TIME ZONE"),
    # Oracle DATE carries a time component -> map to TIMESTAMP in Postgres.
    (re.compile(r"\bDATE\b", re.IGNORECASE), "TIMESTAMP"),
]

# HEXTORAW('AABB') -> '\xAABB'  (Postgres bytea hex literal). This one spans
# code + a quoted argument, so it is applied to the FULL line (see _convert_line),
# NOT inside _apply_rules_outside_literals.
_HEXTORAW_RE = re.compile(r"HEXTORAW\s*\(\s*'([0-9A-Fa-f]*)'\s*\)")

# Function / expression rewrites applied ONLY to code OUTSIDE string literals and
# quoted identifiers (so a literal 'SYSDATE' / 'FROM DUAL' is never corrupted).
# HEXTORAW is intentionally excluded here — it is handled on the full line.
_FUNC_RULES_NOLIT = [
    (re.compile(r"\bSYSDATE\b", re.IGNORECASE), "CURRENT_TIMESTAMP"),
    (re.compile(r"\bSYSTIMESTAMP\b", re.IGNORECASE), "CURRENT_TIMESTAMP"),
    (re.compile(r"\bNVL\s*\(", re.IGNORECASE), "COALESCE("),
    (re.compile(r"\bSYS_GUID\s*\(\s*\)", re.IGNORECASE), "gen_random_uuid()"),
    # Oracle INSTR(str, sub) and Postgres strpos(str, sub) share the SAME argument
    # order and both return the 1-based position (0 if not found), so the common
    # 2-arg form converts faithfully. The 3-/4-arg Oracle form (start position /
    # occurrence) has no direct strpos equivalent and will still fail ADD
    # CONSTRAINT — but it is now caught by the per-statement isolation in
    # migrate_table_constraints, so it only drops that one CHECK, never the PK.
    (re.compile(r"\bINSTR\s*\(", re.IGNORECASE), "strpos("),
    # Oracle string concat is the same (||) so nothing to do there.
    # FROM DUAL is meaningless in Postgres.
    (re.compile(r"\bFROM\s+DUAL\b", re.IGNORECASE), ""),
]

# Back-compat alias: the full set of function rules (used to be applied whole-line).
_FUNC_RULES = _FUNC_RULES_NOLIT + [(_HEXTORAW_RE, r"'\\x\1'")]

# TO_DATE / TO_TIMESTAMP with the formats we emit in step 1 -> Postgres casts.
_TO_TIMESTAMP_RE = re.compile(
    r"TO_TIMESTAMP\s*\(\s*'([^']*)'\s*,\s*'[^']*'\s*\)", re.IGNORECASE
)
_TO_DATE_RE = re.compile(
    r"TO_DATE\s*\(\s*'([^']*)'\s*,\s*'[^']*'\s*\)", re.IGNORECASE
)


# Splits a line into alternating (code, literal) regions so the function/type
# rules never rewrite text *inside* a single-quoted string literal ('' escaping)
# or a double-quoted identifier ("" escaping). Each match is one whole literal /
# identifier region; everything between matches is "code" we are free to rewrite.
_LITERAL_REGION_RE = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")


def _apply_rules_outside_literals(line: str, rules) -> str:
    """Apply (pattern, repl) rules only to the parts of ``line`` that are NOT
    inside a single-quoted string literal or double-quoted identifier."""
    out = []
    pos = 0
    for m in _LITERAL_REGION_RE.finditer(line):
        code = line[pos:m.start()]
        for pattern, repl in rules:
            code = pattern.sub(repl, code)
        out.append(code)
        out.append(m.group(0))  # literal / identifier region: verbatim
        pos = m.end()
    tail = line[pos:]
    for pattern, repl in rules:
        tail = pattern.sub(repl, tail)
    out.append(tail)
    return "".join(out)


def _convert_line(line: str, in_ddl: bool) -> str:
    """Apply the conversion rules to a single line of SQL.

    Order is deliberate:
      1. TO_DATE / TO_TIMESTAMP / HEXTORAW run on the FULL line first. Each of
         these matches a function call whose quoted argument is *part of the
         match* (e.g. ``TO_DATE('2024-01-01','YYYY-MM-DD')``), so they must see
         the whole token — splitting on literals would break them.
      2. The remaining function rules (SYSDATE, NVL, FROM DUAL, ...) and, in DDL,
         the type rules, run ONLY on the code regions outside string literals /
         quoted identifiers. This stops a literal like the DEFAULT ``'SYSDATE'``
         or a CHECK value ``'FROM DUAL'`` from being silently rewritten.
    """
    # 1. Date/time + HEXTORAW conversions: consume their own quoted args by design.
    line = _TO_TIMESTAMP_RE.sub(r"TIMESTAMP '\1'", line)
    line = _TO_DATE_RE.sub(r"DATE '\1'", line)
    line = _HEXTORAW_RE.sub(r"'\\x\1'", line)

    # 2. Function rewrites + (DDL-only) type rewrites, literal-aware.
    rules = list(_FUNC_RULES_NOLIT)
    if in_ddl:
        rules += _TYPE_RULES
    line = _apply_rules_outside_literals(line, rules)

    return line


def convert_oracle_sql_to_postgres(oracle_sql_path: str, table_name: str) -> str:
    """
    STEP 2: Convert an Oracle .sql file into a Postgres .sql file.
    Returns the path to the converted file.
    """
    out_path = os.path.join(POSTGRES_DUMP_DIR, f"{table_name}.sql")

    with open(oracle_sql_path, "r", encoding="utf-8") as src, \
         open(out_path, "w", encoding="utf-8") as dst:

        dst.write(f"-- Converted from Oracle dump: {os.path.basename(oracle_sql_path)}\n")
        dst.write(f"SET search_path TO {PG_SCHEMA};\n\n")

        in_ddl = False
        for raw_line in src:
            line = raw_line

            # Track whether we are inside a CREATE TABLE block so type rules apply there.
            stripped = line.strip().upper()
            if stripped.startswith("CREATE TABLE"):
                in_ddl = True
            line = _convert_line(line, in_ddl)
            # End the CREATE TABLE block only when the closing ");" appears OUTSIDE
            # any string literal / quoted identifier. A naive `");" in line` check
            # mis-fires on a column DEFAULT whose literal contains ");" (e.g.
            # DEFAULT 'x);y'), flipping in_ddl off mid-table so every later column
            # skips the type rules and emits NUMBER/DATE verbatim -> CREATE fails ->
            # table lost. Mask quoted regions first (same regex the rule engine uses)
            # so only the structural closing paren can end the block.
            if in_ddl and ");" in _LITERAL_REGION_RE.sub("", line):
                in_ddl = False

            # Oracle uses "" for quoting; Postgres uses "" too, so identifiers pass through.
            dst.write(line)

    print(f"  [convert] {table_name}: {oracle_sql_path} -> {out_path}")
    return out_path


def convert_oracle_sql_text(oracle_sql_text: str) -> str:
    """
    Convert a chunk of Oracle SQL text (e.g. constraint / index DDL) to Postgres.
    Used by the constraints pass. Type rules are skipped (no column type defs here);
    function rules (NVL, SYSDATE, ...) still apply, which matters for CHECK conditions.
    """
    return "".join(_convert_line(line, in_ddl=False) for line in oracle_sql_text.splitlines(keepends=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. STEP 3 — Prepare the target per `LOAD_MODE`, then bulk-load the CSV via `COPY`
# MAGIC
# MAGIC `COPY` is used instead of executing per-row `INSERT`s — it streams the whole CSV into
# MAGIC Postgres in one operation and is typically 10–100× faster on large tables.
# MAGIC
# MAGIC **`LOAD_MODE`** controls what happens to a pre-existing target table:
# MAGIC - `"recreate"` (default) — DROP (CASCADE) + CREATE from the converted DDL, then load. Runs
# MAGIC   the structural passes (constraints/indexes/identity/sequences/FKs).
# MAGIC - `"truncate"` — table must already exist; TRUNCATE it, then load **data only** (no DDL,
# MAGIC   no structural passes).
# MAGIC - `"append"` — table must already exist; load **data only**, no truncate (rows are added).
# MAGIC
# MAGIC In the data-only modes a preflight checks the table exists and its columns cover the data.

# COMMAND ----------

def _split_sql_statements(sql_text: str):
    """
    Split a SQL script into individual statements on semicolons, ignoring
    semicolons inside single-quoted string literals ('' escaping), double-quoted
    identifiers ("" escaping), and dollar-quoted strings ($tag$ ... $tag$).
    """
    statements = []
    buf = []
    in_string = False        # inside '...'
    in_ident = False         # inside "..."
    dollar_tag = None        # the active $tag$ delimiter, or None
    i = 0
    n = len(sql_text)
    _dollar_re = re.compile(r"\$[A-Za-z_0-9]*\$")
    while i < n:
        ch = sql_text[i]

        # Dollar-quoted string: match opening/closing $tag$ only when not in a quote.
        if not in_string and not in_ident:
            m = _dollar_re.match(sql_text, i)
            if m:
                tok = m.group(0)
                if dollar_tag is None:
                    dollar_tag = tok
                elif dollar_tag == tok:
                    dollar_tag = None
                buf.append(tok)
                i = m.end()
                continue

        if dollar_tag is not None:
            buf.append(ch)
            i += 1
            continue

        buf.append(ch)
        if ch == "'" and not in_ident:
            if in_string and i + 1 < n and sql_text[i + 1] == "'":
                buf.append(sql_text[i + 1])
                i += 2
                continue
            in_string = not in_string
        elif ch == '"' and not in_string:
            if in_ident and i + 1 < n and sql_text[i + 1] == '"':
                buf.append(sql_text[i + 1])
                i += 2
                continue
            in_ident = not in_ident
        elif ch == ";" and not in_string and not in_ident:
            stmt = "".join(buf).strip()
            if stmt and stmt != ";":
                statements.append(stmt.rstrip(";").strip())
            buf = []
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail.rstrip(";").strip())
    return [s for s in statements if s]


def _assert_target_ready(cur, table_name: str, csv_columns):
    """For data-only modes: verify the target table exists and has the CSV's columns."""
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (PG_SCHEMA, table_name),
    )
    existing = {r[0] for r in cur.fetchall()}
    if not existing:
        raise RuntimeError(
            f'LOAD_MODE="{LOAD_MODE}" but target table "{PG_SCHEMA}"."{table_name}" '
            f"does not exist. Create it first, or use LOAD_MODE=\"recreate\"."
        )
    missing = [c for c in csv_columns if c not in existing]
    if missing:
        raise RuntimeError(
            f'Target "{PG_SCHEMA}"."{table_name}" is missing column(s) {missing} '
            f"present in the Oracle data. Column names must match for COPY."
        )


def load_postgres_copy(pg_conn, postgres_ddl_path: str, csv_path: str, table_name: str):
    """
    STEP 3: Prepare the target table per LOAD_MODE, then bulk-load the CSV with COPY.

    LOAD_MODE:
      "recreate" — DROP (CASCADE) + CREATE from the converted DDL, then load.
      "truncate" — TRUNCATE the existing table, then load (data only).
      "append"   — load into the existing table as-is (data only).

    The CSV's first line is a header giving the column order.
    """
    cur = pg_conn.cursor()
    # _pg_qualified applies _short_ident, so the DROP/COPY target name matches the
    # (possibly shortened) name CREATE TABLE emitted. No-op for names <= 63 bytes.
    qualified = _pg_qualified(table_name)
    try:
        if LOAD_MODE == "recreate":
            cur.execute(f"DROP TABLE IF EXISTS {qualified} CASCADE;")
            with open(postgres_ddl_path, "r", encoding="utf-8") as f:
                for stmt in _split_sql_statements(f.read()):
                    cur.execute(stmt)

        # Bulk-load the data. Read the header to pin the column order, then COPY the rest.
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            header = next(csv.reader([f.readline()]))

            if LOAD_MODE in ("truncate", "append"):
                _assert_target_ready(cur, table_name, header)
            if LOAD_MODE == "truncate":
                cur.execute(f"TRUNCATE TABLE {qualified};")

            # Shorten each header (real Oracle) column name the same way CREATE TABLE
            # did, so the COPY column list matches the emitted column names.
            col_list = ", ".join(_pg_ident(c) for c in header)
            # FORCE_NULL on every loaded column so a *quoted* empty field ("") is
            # also read as NULL, not as an empty string. csv.writer emits a lone
            # empty field on a single-column row as a quoted "" (to avoid writing a
            # blank line); plain `NULL ''` only matches the UNQUOTED empty token, so
            # without FORCE_NULL a single-column NULL would silently load as ''.
            # This is semantically correct for Oracle-sourced data: Oracle treats ''
            # as NULL, so there is never a genuine empty string to preserve.
            copy_sql = (
                f"COPY {qualified} ({col_list}) "
                f"FROM STDIN WITH (FORMAT csv, HEADER false, NULL '', "
                f"FORCE_NULL ({col_list}))"
            )
            cur.copy_expert(copy_sql, f)      # f is now positioned just after the header
            row_count = cur.rowcount

        pg_conn.commit()
        print(f"  [load]    {table_name}: COPY {row_count} rows ({LOAD_MODE})")
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cur.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. SECOND PASS — Primary keys, unique/check constraints, indexes & foreign keys
# MAGIC
# MAGIC The first pass only carries column definitions + data. This pass reads the Oracle data
# MAGIC dictionary (`all_constraints`, `all_cons_columns`, `all_indexes`, `all_ind_columns`) and
# MAGIC reproduces the relational structure on Postgres:
# MAGIC
# MAGIC - **PK / UNIQUE / CHECK** and **indexes** are applied per-table (they only reference one table).
# MAGIC - **FOREIGN KEYs** are collected across all tables and applied **last**, after every table and
# MAGIC   its data exist, so referential integrity doesn't fail on table/row ordering.
# MAGIC
# MAGIC As with the data pass, DDL is dumped Oracle-flavoured ➜ converted ➜ loaded.

# COMMAND ----------

def _pg_ident(name: str) -> str:
    """Quote an identifier for Postgres, shortening it if it exceeds 63 bytes.

    `_short_ident` is a no-op for any name that already fits in 63 bytes, so this
    is transparent for normal identifiers and only kicks in for over-long ones —
    keeping the emitted name consistent with CREATE TABLE / COPY / constraints."""
    return '"' + _short_ident(name).replace('"', '""') + '"'


def _pg_qualified(table_name: str) -> str:
    return f"{_pg_ident(PG_SCHEMA)}.{_pg_ident(table_name)}"


def _pg_column_type(pg_cur, table_name: str, column_name: str):
    """Return the Postgres data_type for a column, or None if it doesn't exist.

    The table/column may have been shortened on the Postgres side (see
    _short_ident), so we look up by the shortened name to find the real row.
    No-op for names <= 63 bytes."""
    pg_cur.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (PG_SCHEMA, _short_ident(table_name), _short_ident(column_name)),
    )
    row = pg_cur.fetchone()
    return row[0] if row else None


def _table_columns(ora_conn, owner: str, table_name: str):
    """All column names for a table (used to quote identifiers in CHECK conditions)."""
    cur = ora_conn.cursor()
    cur.execute(
        """
        SELECT column_name
        FROM all_tab_columns
        WHERE owner = :owner AND table_name = :tname
        """,
        owner=owner, tname=table_name,
    )
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


# Matches: a double-quoted identifier, OR a single-quoted string literal
# (Oracle '' escaping), OR a bare identifier. Order matters so quoted regions
# are consumed whole and never rewritten.
_CHECK_TOKEN_RE = re.compile(
    r'"[^"]*"|\'(?:[^\']|\'\')*\'|[A-Za-z_][A-Za-z0-9_$#]*'
)


def _quote_check_condition(cond: str, columns) -> str:
    """
    Quote bare column identifiers in an Oracle CHECK search_condition so they
    survive Postgres' unquoted-folds-to-lowercase rule. Columns are created
    quoted/upper-case, so an unquoted ``ACTIVE`` would fold to ``active`` and
    fail with "column does not exist". String literals and already-quoted
    identifiers are left untouched.
    """
    colset = {c.upper() for c in columns}

    def repl(m):
        tok = m.group(0)
        if tok[0] in ('"', "'"):  # quoted identifier or string literal
            return tok
        if tok.upper() in colset:
            # Don't quote a token that is actually a function call, i.e. the next
            # non-whitespace char after the match is '('. e.g. in LENGTH(NAME)>0,
            # LENGTH happens to share a name with a column but is a function here.
            # A real column operand like ``ACTIVE IN (...)`` has ``IN`` (not '(')
            # as its next non-whitespace token, so it is still quoted.
            if cond[m.end():].lstrip()[:1] == "(":
                return tok
            # Shorten so the reference matches a column that CREATE TABLE shortened.
            # No-op for names <= 63 bytes.
            return '"' + _short_ident(tok.upper()) + '"'
        return tok

    return _CHECK_TOKEN_RE.sub(repl, cond)


def _constraint_columns(ora_conn, owner: str, constraint_name: str):
    """Ordered column list for a given constraint."""
    cur = ora_conn.cursor()
    cur.execute(
        """
        SELECT column_name
        FROM all_cons_columns
        WHERE owner = :owner AND constraint_name = :cname
        ORDER BY position
        """,
        owner=owner, cname=constraint_name,
    )
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


def build_oracle_constraint_ddl(ora_conn, table_name: str):
    """
    Read PK / UNIQUE / CHECK constraints and indexes for a table and return
    Oracle-flavoured DDL (str). Foreign keys are intentionally excluded here.
    """
    owner = ORACLE_SCHEMA.upper()
    # Emit the Postgres-side (possibly shortened) table name so it matches the
    # CREATE TABLE / COPY target. No-op for names <= 63 bytes.
    qualified = f'"{_short_ident(table_name)}"'
    lines = [f"-- constraints & indexes for {table_name}"]
    table_cols = _table_columns(ora_conn, owner, table_name)

    cur = ora_conn.cursor()

    # --- PK / UNIQUE / CHECK constraints ---
    if MIGRATE_PK_UNIQUE_CHECK:
        cur.execute(
            """
            SELECT constraint_name, constraint_type, search_condition
            FROM all_constraints
            WHERE owner = :owner AND table_name = :tname
              AND constraint_type IN ('P', 'U', 'C')
              AND status = 'ENABLED'
            ORDER BY constraint_type, constraint_name
            """,
            owner=owner, tname=table_name,
        )
        for cname, ctype, search_cond in cur.fetchall():
            if ctype in ("P", "U"):
                cols = _constraint_columns(ora_conn, owner, cname)
                if not cols:
                    continue
                kind = "PRIMARY KEY" if ctype == "P" else "UNIQUE"
                col_list = ", ".join(f'"{_short_ident(c)}"' for c in cols)
                lines.append(
                    f'ALTER TABLE {qualified} ADD CONSTRAINT '
                    f'"{_short_ident(cname)}" {kind} ({col_list});'
                )
            elif ctype == "C":
                cond = (search_cond or "").strip()
                # Skip the system-generated "COL" IS NOT NULL checks (already NOT NULL in DDL).
                if not cond or re.match(r'^"?\w+"?\s+IS\s+NOT\s+NULL$', cond, re.IGNORECASE):
                    continue
                cond = _quote_check_condition(cond, table_cols)
                lines.append(
                    f'ALTER TABLE {qualified} ADD CONSTRAINT '
                    f'"{_short_ident(cname)}" CHECK ({cond});'
                )

    # --- Indexes (excluding those backing PK / UNIQUE constraints) ---
    # Include both plain ('NORMAL') and function-based / descending indexes
    # ('FUNCTION-BASED NORMAL'). Oracle stores BOTH (col DESC) and (UPPER(col))
    # as FUNCTION-BASED NORMAL; Postgres supports descending AND expression
    # indexes, so emitting them recovers fidelity that was previously dropped.
    if MIGRATE_INDEXES:
        cur.execute(
            """
            SELECT index_name, uniqueness
            FROM all_indexes
            WHERE table_owner = :owner AND table_name = :tname
              AND index_type IN ('NORMAL', 'FUNCTION-BASED NORMAL')
              AND index_name NOT IN (
                  SELECT constraint_name FROM all_constraints
                  WHERE owner = :owner AND table_name = :tname
                    AND constraint_type IN ('P', 'U')
              )
            ORDER BY index_name
            """,
            owner=owner, tname=table_name,
        )
        index_rows = cur.fetchall()
        for index_name, uniqueness in index_rows:
            icur = ora_conn.cursor()
            # Pull each key column with its sort direction AND, for function-based
            # indexes, the expression text (a LONG in all_ind_expressions). Plain
            # (col DESC) indexes are also function-based: Oracle stores them with
            # column_expression='"COL"' and descend='DESC', so the expression path
            # handles DESC naturally.
            icur.execute(
                """
                SELECT ic.column_name, ic.descend, ie.column_expression
                FROM all_ind_columns ic
                LEFT JOIN all_ind_expressions ie
                  ON ie.index_owner = ic.index_owner
                 AND ie.index_name  = ic.index_name
                 AND ie.column_position = ic.column_position
                WHERE ic.index_owner = :owner AND ic.index_name = :iname
                ORDER BY ic.column_position
                """,
                owner=owner, iname=index_name,
            )
            keys = []
            for col_name, descend, col_expr in icur.fetchall():
                if col_expr is not None:
                    # Expression text references already-quoted Oracle column
                    # names (e.g. UPPER("NAME")). Convert it so function rules +
                    # literal-awareness apply. >63-char columns inside the
                    # expression are an edge-of-edge, left as-is.
                    key = convert_oracle_sql_text(str(col_expr)).strip()
                else:
                    key = f'"{_short_ident(col_name)}"'
                if (descend or "").upper() == "DESC":
                    key += " DESC"
                keys.append(key)
            icur.close()
            if not keys:
                continue
            unique = "UNIQUE " if uniqueness == "UNIQUE" else ""
            col_list = ", ".join(keys)
            lines.append(
                f'CREATE {unique}INDEX "{_short_ident(index_name)}" '
                f'ON {qualified} ({col_list});'
            )

    cur.close()
    return "\n".join(lines) + "\n"


def build_oracle_foreign_key_ddl(ora_conn, table_name: str):
    """Return Oracle-flavoured FOREIGN KEY DDL (str) for a single table."""
    owner = ORACLE_SCHEMA.upper()
    # Emit the Postgres-side (possibly shortened) table name. No-op for <= 63 bytes.
    qualified = f'"{_short_ident(table_name)}"'
    lines = []

    cur = ora_conn.cursor()
    cur.execute(
        """
        SELECT c.constraint_name, c.r_owner, c.r_constraint_name,
               c.delete_rule, rc.table_name AS ref_table,
               c.deferrable, c.deferred
        FROM all_constraints c
        JOIN all_constraints rc
          ON rc.owner = c.r_owner AND rc.constraint_name = c.r_constraint_name
        WHERE c.owner = :owner AND c.table_name = :tname
          AND c.constraint_type = 'R' AND c.status = 'ENABLED'
        ORDER BY c.constraint_name
        """,
        owner=owner, tname=table_name,
    )
    for (cname, r_owner, r_cname, delete_rule, ref_table,
         deferrable, deferred) in cur.fetchall():
        local_cols = _constraint_columns(ora_conn, owner, cname)
        ref_cols = _constraint_columns(ora_conn, r_owner, r_cname)
        if not local_cols or not ref_cols:
            continue
        local_list = ", ".join(f'"{_short_ident(c)}"' for c in local_cols)
        ref_list = ", ".join(f'"{_short_ident(c)}"' for c in ref_cols)
        # Referenced table is migrated into PG_SCHEMA as well.
        on_delete = ""
        if delete_rule and delete_rule.upper() in ("CASCADE", "SET NULL"):
            on_delete = f" ON DELETE {delete_rule.upper()}"
        # Preserve deferrability: apps relying on deferred checks (circular FKs,
        # bulk-swap inserts) break if a DEFERRABLE constraint becomes immediate.
        deferclause = ""
        if (deferrable or "").upper() == "DEFERRABLE":
            mode = "DEFERRED" if (deferred or "").upper() == "DEFERRED" else "IMMEDIATE"
            deferclause = f" DEFERRABLE INITIALLY {mode}"
        lines.append(
            f'ALTER TABLE {qualified} ADD CONSTRAINT "{_short_ident(cname)}" '
            f'FOREIGN KEY ({local_list}) '
            f'REFERENCES "{_short_ident(ref_table)}" ({ref_list}){on_delete}'
            f'{deferclause};'
        )
    cur.close()
    return "\n".join(lines)


def migrate_table_constraints(ora_conn, pg_conn, table_name: str):
    """Dump ➜ convert ➜ load PK / UNIQUE / CHECK / indexes for one table."""
    if not (MIGRATE_PK_UNIQUE_CHECK or MIGRATE_INDEXES):
        return
    oracle_ddl = build_oracle_constraint_ddl(ora_conn, table_name)
    postgres_ddl = convert_oracle_sql_text(oracle_ddl)

    ora_path = os.path.join(ORACLE_DUMP_DIR, f"{table_name}_constraints.sql")
    pg_path = os.path.join(POSTGRES_DUMP_DIR, f"{table_name}_constraints.sql")
    with open(ora_path, "w", encoding="utf-8") as f:
        f.write(oracle_ddl)
    with open(pg_path, "w", encoding="utf-8") as f:
        f.write(f"SET search_path TO {PG_SCHEMA};\n")
        f.write(postgres_ddl)

    # Apply each statement in its OWN transaction (mirrors migrate_foreign_keys).
    # Oracle CHECKs can use functions with no faithful Postgres equivalent
    # (DECODE, NVL2, TRUNC(date), 3-arg INSTR, ...). If the whole batch ran in a
    # single transaction, ONE bad CHECK would roll back the table's PK / UNIQUE /
    # indexes too. Per-statement isolation means a bad CHECK only loses itself.
    cur = pg_conn.cursor()
    executed, failed = 0, 0
    try:
        for stmt in _split_sql_statements(postgres_ddl):
            # Skip comment-only / empty statements (e.g. a table with no
            # constraints still carries the "-- constraints & indexes for X"
            # header line). Strip SQL line-comments; if nothing real remains,
            # there is no statement to run. Statements that DO carry SQL after a
            # leading comment are executed verbatim (Postgres allows the comment).
            if not re.sub(r"(?m)^\s*--.*$", "", stmt).strip():
                continue
            try:
                cur.execute(stmt)
                pg_conn.commit()
                executed += 1
            except Exception as exc:  # noqa: BLE001
                pg_conn.rollback()
                failed += 1
                gist = " ".join(stmt.split())[:120]
                print(f"  [constr ERROR] {table_name}: dropped one statement "
                      f"({exc}): {gist}")
        if executed or failed:
            print(f"  [constr]  {table_name}: applied {executed} constraint/index "
                  f"statement(s)" + (f", {failed} failed" if failed else ""))
    finally:
        cur.close()
        if not KEEP_SQL_FILES:
            os.remove(ora_path)
            os.remove(pg_path)


_PG_INT_TYPES = ("smallint", "integer", "bigint")


def align_foreign_key_column_types(ora_conn, pg_conn, table_names):
    """
    Before applying FKs, make each child FK column's Postgres type match its
    referenced parent column. Oracle NUMBER keys map to NUMERIC, but identity
    parents get coerced to bigint — and Postgres rejects a numeric->bigint FK.
    Only acts when the parent column is an integer type and the child differs.
    """
    owner = ORACLE_SCHEMA.upper()
    cur = pg_conn.cursor()
    aligned = 0
    try:
        for table_name in table_names:
            ocur = ora_conn.cursor()
            ocur.execute(
                """
                SELECT c.constraint_name, c.r_owner, c.r_constraint_name,
                       rc.table_name AS ref_table
                FROM all_constraints c
                JOIN all_constraints rc
                  ON rc.owner = c.r_owner AND rc.constraint_name = c.r_constraint_name
                WHERE c.owner = :owner AND c.table_name = :tname
                  AND c.constraint_type = 'R' AND c.status = 'ENABLED'
                """,
                owner=owner, tname=table_name,
            )
            fks = ocur.fetchall()
            ocur.close()
            for cname, r_owner, r_cname, ref_table in fks:
                local_cols = _constraint_columns(ora_conn, owner, cname)
                ref_cols = _constraint_columns(ora_conn, r_owner, r_cname)
                for lcol, rcol in zip(local_cols, ref_cols):
                    ptype = _pg_column_type(cur, ref_table, rcol)
                    ctype = _pg_column_type(cur, table_name, lcol)
                    if ptype in _PG_INT_TYPES and ctype is not None and ctype != ptype:
                        col = _pg_ident(lcol)
                        try:
                            cur.execute(
                                f"ALTER TABLE {_pg_qualified(table_name)} "
                                f"ALTER COLUMN {col} TYPE {ptype} USING {col}::{ptype};"
                            )
                            pg_conn.commit()
                            aligned += 1
                        except Exception as exc:  # noqa: BLE001
                            pg_conn.rollback()
                            print(f"  [fk type] could not align {table_name}.{lcol} "
                                  f"-> {ptype}: {exc}")
    finally:
        cur.close()
    if aligned:
        print(f"Foreign keys: aligned {aligned} child column type(s) to parent")


def migrate_foreign_keys(ora_conn, pg_conn, table_names):
    """
    Final pass: dump ➜ convert ➜ load all FOREIGN KEYs once every table exists.
    Each FK is applied independently so one bad FK doesn't abort the rest.
    """
    if not MIGRATE_FOREIGN_KEYS:
        return 0

    # Type-align child FK columns to their (possibly bigint-coerced) parents first.
    align_foreign_key_column_types(ora_conn, pg_conn, table_names)

    all_fk_lines = []
    for table_name in table_names:
        ddl = build_oracle_foreign_key_ddl(ora_conn, table_name)
        if ddl.strip():
            all_fk_lines.append(ddl)

    if not all_fk_lines:
        print("No foreign keys to migrate.")
        return 0

    oracle_ddl = "-- foreign keys (all tables)\n" + "\n".join(all_fk_lines) + "\n"
    postgres_ddl = convert_oracle_sql_text(oracle_ddl)

    ora_path = os.path.join(ORACLE_DUMP_DIR, "_foreign_keys.sql")
    pg_path = os.path.join(POSTGRES_DUMP_DIR, "_foreign_keys.sql")
    with open(ora_path, "w", encoding="utf-8") as f:
        f.write(oracle_ddl)
    with open(pg_path, "w", encoding="utf-8") as f:
        f.write(f"SET search_path TO {PG_SCHEMA};\n")
        f.write(postgres_ddl)

    applied, failed = 0, 0
    cur = pg_conn.cursor()
    for stmt in _split_sql_statements(postgres_ddl):
        try:
            cur.execute(stmt)
            pg_conn.commit()
            applied += 1
        except Exception as exc:  # noqa: BLE001
            pg_conn.rollback()
            failed += 1
            print(f"  [fk ERROR] {exc}")
    cur.close()
    print(f"Foreign keys: applied {applied}, failed {failed}")
    if not KEEP_SQL_FILES:
        os.remove(ora_path)
        os.remove(pg_path)
    return applied

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5c. THIRD PASS — Sequences & identity columns
# MAGIC
# MAGIC Auto-increment behaviour in Oracle comes in two shapes, both handled here:
# MAGIC
# MAGIC - **Standalone sequences** (`all_sequences`) ➜ Postgres `CREATE SEQUENCE`, started at the
# MAGIC   sequence's current high-water value. Oracle's enormous default `MAXVALUE` (28 nines)
# MAGIC   exceeds Postgres `bigint`, so it is clamped to `NO MAXVALUE` when out of range.
# MAGIC - **Identity columns** (`all_tab_identity_cols`, Oracle 12c+) ➜ the column is altered to
# MAGIC   `GENERATED {ALWAYS|BY DEFAULT} AS IDENTITY` *after* its data is loaded, then `RESTART WITH
# MAGIC   MAX(col)+1` so future inserts don't collide with migrated rows.
# MAGIC
# MAGIC > Pre-12c "sequence + BEFORE INSERT trigger" auto-increment is **not** auto-detected — convert
# MAGIC > those columns to identity manually or rely on the migrated standalone sequence.

# COMMAND ----------

# Postgres bigint bounds — Oracle sequences can exceed these.
_PG_BIGINT_MAX = 9223372036854775807
_PG_BIGINT_MIN = -9223372036854775808


def migrate_sequences(ora_conn, pg_conn):
    """Re-create every standalone Oracle sequence in Postgres (run once)."""
    if not MIGRATE_SEQUENCES:
        return 0

    cur = ora_conn.cursor()
    cur.execute(
        # Raw string: the LIKE pattern needs a literal backslash (ESCAPE '\') so
        # the '_' in 'ISEQ$$_%' is treated literally, not as a wildcard. Filters
        # out Oracle's internal IDENTITY-backing sequences (e.g. ISEQ$$_75929),
        # which are handled by migrate_identity_columns — not as standalone seqs.
        r"""
        SELECT sequence_name, min_value, max_value, increment_by,
               cycle_flag, cache_size, last_number
        FROM all_sequences
        WHERE sequence_owner = :owner
          AND sequence_name NOT LIKE 'ISEQ$$\_%' ESCAPE '\'
        ORDER BY sequence_name
        """,
        owner=ORACLE_SCHEMA.upper(),
    )
    rows = cur.fetchall()
    cur.close()

    if not rows:
        print("No sequences to migrate.")
        return 0

    statements = []
    for name, min_v, max_v, incr, cycle_flag, cache, last_number in rows:
        min_v = int(min_v)
        max_v = int(max_v)
        incr = int(incr or 1)
        start = int(last_number or min_v)

        minclause = f"MINVALUE {min_v}" if min_v >= _PG_BIGINT_MIN else "NO MINVALUE"
        maxclause = f"MAXVALUE {max_v}" if max_v <= _PG_BIGINT_MAX else "NO MAXVALUE"
        # Clamp the start value into the representable range as well. If the real
        # Oracle high-water mark exceeds Postgres bigint, clamping it DOWN means
        # the next Postgres-generated value could collide with already-migrated
        # keys. We can't represent it, so warn loudly and name the sequence.
        if start > _PG_BIGINT_MAX or start < _PG_BIGINT_MIN:
            print(
                f"  [seq WARNING] {name}: Oracle next value {start} exceeds Postgres "
                f"bigint range; clamping START to "
                f"{_PG_BIGINT_MAX if start > _PG_BIGINT_MAX else _PG_BIGINT_MIN}. "
                f"Future inserts may COLLIDE with migrated keys — review this sequence."
            )
        # Clamp START into [MINVALUE, MAXVALUE] FIRST: a wrapped CYCLE sequence or a
        # lowered MAXVALUE can leave the Oracle high-water mark above MAXVALUE, which
        # Postgres rejects ("START value cannot be greater than MAXVALUE"). Then clamp
        # into the representable bigint range.
        start = min(max(start, min_v), max_v)
        start = max(min(start, _PG_BIGINT_MAX), _PG_BIGINT_MIN)
        cacheclause = f"CACHE {int(cache)}" if cache and int(cache) > 1 else "CACHE 1"
        cycleclause = "CYCLE" if (cycle_flag or "N").upper() == "Y" else "NO CYCLE"

        statements.append((
            name,
            f'CREATE SEQUENCE IF NOT EXISTS {_pg_qualified(name)} '
            f'INCREMENT BY {incr} {minclause} {maxclause} '
            f'START WITH {start} {cacheclause} {cycleclause};'
        ))

    # Dump ➜ (no conversion needed, already Postgres) ➜ load.
    sql_text = (
        f"SET search_path TO {PG_SCHEMA};\n"
        + "\n".join(stmt for _, stmt in statements) + "\n"
    )
    pg_path = os.path.join(POSTGRES_DUMP_DIR, "_sequences.sql")
    with open(pg_path, "w", encoding="utf-8") as f:
        f.write(sql_text)

    # Apply each CREATE SEQUENCE in its OWN transaction (mirrors
    # migrate_table_constraints / migrate_foreign_keys). migrate_sequences runs
    # FIRST in migrate_all_tables; a single bad sequence must NOT roll back the
    # whole batch or abort the migration before any table loads. Log and continue.
    applied, failed = 0, 0
    cur = pg_conn.cursor()
    try:
        for name, stmt in statements:
            try:
                cur.execute(stmt)
                pg_conn.commit()
                applied += 1
            except Exception as exc:  # noqa: BLE001
                pg_conn.rollback()
                failed += 1
                print(f"  [seq ERROR] {name}: {exc}")
    finally:
        cur.close()
        if not KEEP_SQL_FILES:
            os.remove(pg_path)
    print(f"Sequences: created {applied}" + (f", {failed} failed" if failed else ""))
    return applied


def migrate_identity_columns(ora_conn, pg_conn, table_name: str):
    """Convert Oracle IDENTITY columns on one table to Postgres IDENTITY columns."""
    if not MIGRATE_IDENTITY_COLUMNS:
        return

    cur = ora_conn.cursor()
    cur.execute(
        """
        SELECT column_name, generation_type
        FROM all_tab_identity_cols
        WHERE owner = :owner AND table_name = :tname
        """,
        owner=ORACLE_SCHEMA.upper(), tname=table_name,
    )
    identity_cols = cur.fetchall()
    cur.close()
    if not identity_cols:
        return

    qualified = _pg_qualified(table_name)
    pgcur = pg_conn.cursor()
    try:
        for column_name, generation_type in identity_cols:
            col = _pg_ident(column_name)
            gen = "ALWAYS" if (generation_type or "").upper() == "ALWAYS" else "BY DEFAULT"

            # Find the current max so the identity sequence resumes past migrated data.
            pgcur.execute(f"SELECT COALESCE(MAX({col}), 0) FROM {qualified}")
            current_max = int(pgcur.fetchone()[0] or 0)

            # Postgres IDENTITY (and sequences) are bigint-bound. An Oracle NUMBER key
            # holding values beyond bigint can't be a PG identity at all — leave it as
            # NUMERIC (data preserved) and warn rather than crash on an overflowing cast.
            if current_max > _PG_BIGINT_MAX:
                print(f"  [ident]   SKIP {table_name}.{column_name}: max {current_max} "
                      f"exceeds bigint; left as NUMERIC without identity.")
                continue

            # Oracle identity columns are NUMBER, which this converter maps to NUMERIC.
            # Postgres IDENTITY only accepts smallint/integer/bigint, so coerce to bigint
            # (data is already loaded; the cast is safe for the integer values Oracle stores).
            pgcur.execute(
                f"ALTER TABLE {qualified} ALTER COLUMN {col} "
                f"TYPE bigint USING {col}::bigint;"
            )
            pgcur.execute(
                f"ALTER TABLE {qualified} ALTER COLUMN {col} "
                f"ADD GENERATED {gen} AS IDENTITY;"
            )
            pgcur.execute(
                f"ALTER TABLE {qualified} ALTER COLUMN {col} "
                f"RESTART WITH {int(current_max) + 1};"
            )
            print(f"  [ident]   {table_name}.{column_name}: GENERATED {gen} AS IDENTITY "
                  f"(restart {int(current_max) + 1})")
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        pgcur.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5d. Dependency-aware load ordering
# MAGIC
# MAGIC Some tables must load before others (parent before child). We read the foreign-key graph
# MAGIC from `all_constraints` (type `R`) — **restricted to the tables being migrated** — and
# MAGIC topologically sort it so every parent loads before its children (Kahn's algorithm).
# MAGIC
# MAGIC Robustness details:
# MAGIC - **Self-references** (a table FK'ing itself) are ignored for ordering — a single table is
# MAGIC   always loadable on its own; the FK is satisfied later by the deferred FK pass.
# MAGIC - **Cycles** (e.g. A→B→A) cannot be fully ordered. They are detected, logged, and broken by
# MAGIC   emitting the lowest-remaining-dependency tables in a stable order; those rows rely on the
# MAGIC   deferred FK pass for integrity.
# MAGIC - **Deterministic**: ties broken alphabetically, so the same input always yields the same order.
# MAGIC - FKs pointing at tables **outside** the migration set are ignored for ordering (we can't load
# MAGIC   what we're not migrating), but are reported so you know they exist.

# COMMAND ----------

def build_fk_dependency_graph(ora_conn, table_names):
    """
    Build the FK dependency graph for the given tables.

    Returns (deps, external_refs) where:
      deps[child] = set(parents)  — parents that must load before `child`
                                    (self-refs removed; only in-scope parents kept)
      external_refs[child] = set(parents outside the migration set)
    """
    in_scope = {t.upper() for t in table_names}
    owner = ORACLE_SCHEMA.upper()

    deps = {t: set() for t in table_names}
    # Map uppercase -> original spelling so we preserve the caller's casing.
    canonical = {t.upper(): t for t in table_names}
    external_refs = {}

    cur = ora_conn.cursor()
    cur.execute(
        """
        SELECT c.table_name AS child_table, rc.table_name AS parent_table
        FROM all_constraints c
        JOIN all_constraints rc
          ON rc.owner = c.r_owner AND rc.constraint_name = c.r_constraint_name
        WHERE c.owner = :owner AND c.constraint_type = 'R' AND c.status = 'ENABLED'
        """,
        owner=owner,
    )
    rows = cur.fetchall()
    cur.close()

    for child_raw, parent_raw in rows:
        child_u, parent_u = child_raw.upper(), parent_raw.upper()
        if child_u not in in_scope:
            continue  # the dependent table isn't part of this migration
        if parent_u == child_u:
            continue  # self-reference — not an ordering constraint
        if parent_u in in_scope:
            deps[canonical[child_u]].add(canonical[parent_u])
        else:
            external_refs.setdefault(canonical[child_u], set()).add(parent_raw)

    return deps, external_refs


def order_tables_by_dependency(ora_conn, table_names):
    """
    Return table_names reordered so FK parents load before children (Kahn's algorithm).

    Robust to cycles: when no zero-dependency table remains, the cycle is broken by
    choosing the remaining table with the fewest unmet dependencies (ties alphabetical),
    which is logged. Output always contains exactly the input tables, once each.
    """
    deps, external_refs = build_fk_dependency_graph(ora_conn, table_names)

    if external_refs:
        print("Note: FKs referencing tables OUTSIDE the migration set (ignored for ordering):")
        for child, parents in sorted(external_refs.items()):
            print(f"  - {child} -> {', '.join(sorted(parents))}")

    # Work on a mutable copy of the dependency sets.
    remaining = {t: set(parents) for t, parents in deps.items()}
    ordered = []
    placed = set()
    cycles_broken = []

    while remaining:
        # Tables whose parents are all already placed.
        ready = sorted(t for t, parents in remaining.items() if not (parents - placed))
        if not ready:
            # Cycle (or mutual dependency): break it deterministically.
            unmet = lambda t: len(remaining[t] - placed)
            victim = sorted(remaining, key=lambda t: (unmet(t), t))[0]
            cycles_broken.append(victim)
            ready = [victim]

        for t in ready:
            ordered.append(t)
            placed.add(t)
            del remaining[t]

    if cycles_broken:
        print("WARNING: FK dependency cycle(s) detected. Order forced for these tables "
              "(they rely on the deferred FK pass for integrity):")
        for t in cycles_broken:
            print(f"  - {t}")

    return ordered


def resolve_load_order(ora_conn, table_names):
    """Apply RESPECT_LOAD_ORDER: keep the given order, or sort by FK dependency."""
    if RESPECT_LOAD_ORDER:
        print("Load order: using TABLE_NAMES order as-is (RESPECT_LOAD_ORDER=True).")
        return list(table_names)
    print("Load order: resolving FK dependencies (parents before children)...")
    ordered = order_tables_by_dependency(ora_conn, table_names)
    print(f"Load order resolved for {len(ordered)} tables.")
    return ordered

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Orchestration — parallel fan-out (thread pool) with bracketing sequence/FK passes
# MAGIC
# MAGIC Tables are migrated concurrently with a thread pool of `MAX_PARALLEL_TABLES` workers. Each
# MAGIC worker runs the full per-table pipeline (dump ➜ convert ➜ COPY ➜ constraints ➜ identity)
# MAGIC on its **own** Oracle + Postgres connections — the drivers are not safe to share across
# MAGIC threads. The work is I/O-bound (DB + file I/O releases the GIL), so threads give real
# MAGIC concurrency and overlap Oracle reads with Postgres writes across different tables.
# MAGIC
# MAGIC Sequences run once **before** the pool; foreign keys run once **after** it (deferred-FK
# MAGIC design), so load order is irrelevant to correctness and tables can finish in any order.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed


def discover_oracle_tables(ora_conn):
    """Return all table names owned by ORACLE_SCHEMA."""
    cur = ora_conn.cursor()
    cur.execute(
        "SELECT table_name FROM all_tables WHERE owner = :owner ORDER BY table_name",
        owner=ORACLE_SCHEMA.upper(),
    )
    names = [r[0] for r in cur.fetchall()]
    cur.close()
    return names


def migrate_one_table(table_name: str):
    """
    Full per-table pipeline on dedicated connections (safe to run in a worker thread).
    Returns (table_name, ok: bool, error: str | None, seconds: float).
    """
    t0 = datetime.datetime.now()
    ora_conn = get_oracle_connection()
    pg_conn = get_postgres_connection()
    try:
        ddl_path, csv_path = dump_oracle_table(ora_conn, table_name)        # STEP 1
        pg_ddl_path = convert_oracle_sql_to_postgres(ddl_path, table_name)  # STEP 2
        load_postgres_copy(pg_conn, pg_ddl_path, csv_path, table_name)      # STEP 3
        if LOAD_MODE == "recreate":
            # Structural passes only when we built the table; in data-only modes
            # (truncate/append) the target schema already has these.
            migrate_table_constraints(ora_conn, pg_conn, table_name)       # PK/UNIQUE/CHECK/index
            migrate_identity_columns(ora_conn, pg_conn, table_name)        # identity columns

        if not KEEP_SQL_FILES:
            for p in (ddl_path, csv_path, pg_ddl_path):
                try:
                    os.remove(p)
                except OSError:
                    pass

        elapsed = (datetime.datetime.now() - t0).total_seconds()
        return (table_name, True, None, elapsed)
    except Exception as exc:  # noqa: BLE001
        elapsed = (datetime.datetime.now() - t0).total_seconds()
        return (table_name, False, str(exc), elapsed)
    finally:
        ora_conn.close()
        pg_conn.close()


def migrate_all_tables():
    """Run the full dump ➜ convert ➜ COPY pipeline for every table, in parallel."""
    if LOAD_MODE not in ("recreate", "truncate", "append"):
        raise ValueError(f'LOAD_MODE must be "recreate", "truncate" or "append", got "{LOAD_MODE}"')

    setup_ora = get_oracle_connection()
    setup_pg = get_postgres_connection()

    try:
        table_names = discover_oracle_tables(setup_ora) if DISCOVER_TABLES else list(TABLE_NAMES)
        # Ordering is computed for logging/visibility; with parallel fan-out + deferred FKs it is
        # not required for correctness (tables may finish in any order).
        table_names = resolve_load_order(setup_ora, table_names)
        total = len(table_names)
        workers = max(1, int(MAX_PARALLEL_TABLES))
        print(f"Migrating {total} tables with {workers} parallel worker(s) [LOAD_MODE={LOAD_MODE}]\n"
              + "=" * 60)

        results = {"ok": [], "failed": []}
        start_all = datetime.datetime.now()

        # Standalone sequences: create up front (only when building schema, not in data-only modes).
        if LOAD_MODE == "recreate":
            migrate_sequences(setup_ora, setup_pg)
            print("-" * 60)

        # Parallel fan-out: each table is migrated end-to-end on its own connections.
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(migrate_one_table, t): t for t in table_names}
            for fut in as_completed(futures):
                table_name, ok, err, elapsed = fut.result()
                done += 1
                if ok:
                    print(f"[{done}/{total}] [done]  {table_name} in {elapsed:.1f}s")
                    results["ok"].append(table_name)
                else:
                    print(f"[{done}/{total}] [ERROR] {table_name}: {err}")
                    results["failed"].append((table_name, err))

        # FINAL PASS: foreign keys, once every table + its data exists.
        # Only when building schema; data-only modes leave existing FKs untouched.
        if LOAD_MODE == "recreate":
            print("\n" + "-" * 60)
            ok_in_order = [t for t in table_names if t in set(results["ok"])]
            migrate_foreign_keys(setup_ora, setup_pg, ok_in_order)

        total_elapsed = (datetime.datetime.now() - start_all).total_seconds()
        print("\n" + "=" * 60)
        print(f"Finished in {total_elapsed:.1f}s — "
              f"{len(results['ok'])} ok, {len(results['failed'])} failed")
        if results["failed"]:
            print("\nFailed tables:")
            for name, err in results["failed"]:
                print(f"  - {name}: {err}")
            if not CONTINUE_ON_ERROR:
                raise RuntimeError(f"{len(results['failed'])} table(s) failed to migrate")
        return results
    finally:
        setup_ora.close()
        setup_pg.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Run the migration

# COMMAND ----------

results = migrate_all_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. (Optional) Summary as a Spark DataFrame for easy inspection

# COMMAND ----------

summary_rows = (
    [(t, "OK", "") for t in results["ok"]]
    + [(t, "FAILED", err) for t, err in results["failed"]]
)
summary_df = spark.createDataFrame(summary_rows, ["table_name", "status", "error"])
display(summary_df)
