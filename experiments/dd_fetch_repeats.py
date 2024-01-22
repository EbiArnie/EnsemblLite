# Needs duckdb
# From their docs: DuckDB requires Python 3.7 or newer. DuckDB v0.9 does not yet support Python 3.12
import duckdb
from tempfile import TemporaryDirectory
from tqdm import tqdm
import requests

# python -m venv venv
# . venv/bin/activate
# pip install duckdb tqdm requests

# Using public MySQL DBs:
DBHOST = "ensembldb.ensembl.org"
# DBHOST = "asiadb.ensembl.org"
# DBHOST = "useastdb.ensembl.org"

DBPORT = 3306
DBUSER = "anonymous"

DBNAME = "homo_sapiens_core_111_38"
# DBNAME = "bison_bison_bison_core_111_1"
# DBNAME = "saccharomyces_cerevisiae_core_111_4"


# This experiment uses the default homo sapiens, release 111.
FTP_DIR = f"https://ftp.ensembl.org/pub/release-111/mysql/{DBNAME}/"

IMPORT_TABLES = [
    "seq_region",
    "repeat_feature",
    "repeat_consensus",
    "analysis",
    "meta",
    "coord_system"
]

# Open file-based DB, would be in-memory without a file name
CON = duckdb.connect('experiment-repeat.db')


def main():
    print("Preparing a DuckDB in './experiment-repeat.db' for repeat features "
          f"for {DBNAME}, EnsEMBL release 111")

    clean_db()
    read_db_tables()
    print("Table definition import OK")
    print("Fetching dump files")

    # will be removed at end of main
    tmpdir = TemporaryDirectory(prefix="dd_fetch_repeats")
    fetch_tsv_dumps(tmpdir)

    print("Fetch OK")

    ingest_dumps(tmpdir)

    print("Done")


def read_db_tables():
    sql = "INSTALL mysql"
    print(sql)
    sql_ex(sql)

    sql = "LOAD mysql"
    print(sql)
    sql_ex(sql)

    sql = f"ATTACH 'host={DBHOST} port={DBPORT} user={DBUSER} database={DBNAME}' AS mysqldb (TYPE mysql)"
    print(sql)
    sql_ex(sql)

    for tbl in IMPORT_TABLES:
        sql = f"create table {tbl} as select * from mysqldb.{tbl} limit 0"
        print(sql)
        sql_ex(sql)

    sql = "DETACH mysqldb"
    print(sql)
    sql_ex(sql)


def fetch_tsv_dumps(tmpdir):
    url = FTP_DIR
    for tbl in IMPORT_TABLES:
        url = f"{FTP_DIR}{tbl}.txt.gz"
        filepath = f"{tmpdir.name}/{tbl}.txt.gz"

        print(f"Fetching {url}, writing to {filepath}")
        # Streaming, so we can iterate over the response.
        response = requests.get(url, stream=True)

        # Sizes in bytes.
        total_size = int(response.headers.get("content-length", 0))
        block_size = 4096

        with tqdm(total=total_size, unit="B", unit_scale=True, ncols=0) as progress_bar:
            with open(filepath, "wb") as file:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    file.write(data)

        if total_size != 0 and progress_bar.n != total_size:
            raise RuntimeError("Could not download file")


def ingest_dumps(tmpdir):
    for tbl in IMPORT_TABLES:
        filepath = f"{tmpdir.name}/{tbl}.txt.gz"
        print(f"Ingest for table {tbl}")

        sql = f"INSERT INTO {tbl} SELECT * FROM read_csv_auto('{filepath}', nullstr='\\N', header=false, ignore_errors=false, delim='\\t')"
        sql_ex(sql)


def clean_db():
    for tbl in IMPORT_TABLES:
        sql = f"DROP TABLE IF EXISTS {tbl}"
        sql_ex(sql)


def db_size():
    sql = "PRAGMA database_size"
    print(sql_fetchone(sql))


def sql_ex(sql):
    res = CON.execute(sql)
    if res == None:
        raise (Exception("DuckDB error: No result for: {sql}"))


def sql_fetchone(sql):
    res = CON.execute(sql).fetchone()
    if res == None:
        raise (Exception("DuckDB error: No result for: {sql}"))
    return res


main()
