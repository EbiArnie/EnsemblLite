# Needs duckdb
# From their docs: DuckDB requires Python 3.7 or newer. DuckDB v0.9 does not yet support Python 3.12
import duckdb
from tempfile import TemporaryDirectory
from tqdm import tqdm
import requests
import os

# python -m venv venv
# . venv/bin/activate
# pip install duckdb tqdm requests

# The duckdb mysql plugin only seems to work with the ensembldb MySQL servers
DBHOST = "ensembldb.ensembl.org"
DBPORT = 3306
DBUSER = "anonymous"

# DBNAME = "bison_bison_bison_core_111_1"
SPECIES_DBS = ["saccharomyces_cerevisiae_core_111_4",
               "homo_sapiens_core_111_38",
               "bison_bison_bison_core_111_1"]

# This experiment uses release 111.
FTP_DIR = f"https://ftp.ensembl.org/pub/release-111/mysql/"
# local directory name for data output
LOCAL_DIR="data"

REPEAT_TABLES = [
    "seq_region",
    "repeat_feature",
    "repeat_consensus",
]

# Open file-based DB, would be in-memory without a file name
#CON = duckdb.connect('experiment-repeat.db')
CON = duckdb.connect()


def main():
    print("Preparing a DuckDB in './experiment-repeat.db' for repeat features "
          f"for EnsEMBL release 111")


    for dbname in SPECIES_DBS:
        path = f"{LOCAL_DIR}/{dbname}/"
        if os.path.exists(path):
            print(f"Found local data for: {path}")
            continue

        clean_db()
        read_db_tables(dbname)
        print("Table definition import OK, fetching dump files")

        # will be removed at end of main
        tmpdir = TemporaryDirectory(prefix="dd_fetch_repeats")
        fetch_tsv_dumps(dbname, tmpdir)

        print("Fetch OK")

        ingest_dumps(tmpdir)
        filter_and_write_data(dbname)

    for dbname in SPECIES_DBS:
        print()
        print(f"Statistics for: {dbname}")
        outfile = f"{LOCAL_DIR}/{dbname}/repeats.parquet"
        sql = f"SUMMARIZE FROM read_parquet('{outfile}')"
        res = sql_ex(sql)
#        print(res.fetchnumpy())
        print(res.fetchdf())

        print("Done")


def filter_and_write_data(dbname):
    path = f"{LOCAL_DIR}/{dbname}/"
    os.makedirs(path)
    outfile = f"{LOCAL_DIR}/{dbname}/repeats.parquet"
    sql = ("""
        COPY (
            SELECT
                name, repeat_start, repeat_end,
                CASE WHEN seq_region_strand = true THEN 1 ELSE -1 END as strand,
                repeat_type, repeat_class, score
            FROM repeat_feature
            INNER JOIN repeat_consensus USING (repeat_consensus_id)
            INNER JOIN seq_region USING (seq_region_id)
        """
        f") TO '{outfile}' (COMPRESSION ZSTD);"
    )
    sql_ex(sql)



def read_db_tables(dbname):
    sql = "INSTALL mysql"
    sql_ex(sql)

    sql = "LOAD mysql"
    sql_ex(sql)

    sql = f"ATTACH 'host={DBHOST} port={DBPORT} user={DBUSER} database={dbname}' AS mysqldb (TYPE mysql)"
    sql_ex(sql)

    for tbl in REPEAT_TABLES:
        sql = f"create table {tbl} as select * from mysqldb.{tbl} limit 0"
        sql_ex(sql)

    sql = "DETACH mysqldb"
    sql_ex(sql)


def fetch_tsv_dumps(dbname, tmpdir):
    for tbl in REPEAT_TABLES:
        url = f"{FTP_DIR}/{dbname}/{tbl}.txt.gz"
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
    for tbl in REPEAT_TABLES:
        filepath = f"{tmpdir.name}/{tbl}.txt.gz"
        print(f"Ingest for table {tbl}")

        sql = f"INSERT INTO {tbl} SELECT * FROM read_csv_auto('{filepath}', nullstr='\\N', header=false, ignore_errors=false, delim='\\t')"
        sql_ex(sql)


def clean_db():
    for tbl in REPEAT_TABLES:
        sql = f"DROP TABLE IF EXISTS {tbl}"
        sql_ex(sql)


def sql_ex(sql):
    print(sql)
    res = CON.execute(sql)
    if res == None:
        raise (Exception("DuckDB error: No result for: {sql}"))
    return res


def sql_fetchone(sql):
    res = CON.execute(sql).fetchone()
    if res == None:
        raise (Exception("DuckDB error: No result for: {sql}"))
    return res


# Write out complete DB, data and schema
# def write_parquet(dbname):
#     path = f"{LOCAL_DIR}/{dbname}/"
#     os.makedirs(path)
#     sql = f"EXPORT DATABASE '{path}' (FORMAT PARQUET)"
#     sql_ex(sql)


# Show DB size
#def db_size():
#    sql = "PRAGMA database_size"
#    print(sql_fetchone(sql))


main()
