import duckdb
# Usage:
# $ mkdir data
# $ cd data
# Grab the species you want, possibly from another mirror:
# $ wget https://ftp.ensembl.org/pub/release-110/species_EnsemblVertebrates.txt
# $ wget --recursive -e robots=off -nH --no-parent --cut-dirs=3 https://ftp.ensembl.org/pub/release-110/mysql/saccharomyces_cerevisiae_core_110_4/

# TODO
# 1: grab https://github.com/Ensembl/ensembl/blob/release/110/sql/table.sql ,
# parse table definitions with python-sqlparse

# DONE:
# 2: grab species list from
# https://ftp.ensembl.org/pub/release-110/species_EnsemblVertebrates.txt
# and turn into table - quick and dirty version works, see below

# from the table defs and the species data, we know where to find all species
# data and how to ingest
# grab relevant data for species and read into duckdb

# TODO: Is this efficient enough when dealing with bigger species?
# Probably a good idea to at least prepare mysql dumps as parquet files
# There is also https://duckdb.org/docs/extensions/mysql - might be more
# efficient in some cases

# DuckDB is in-memory by default
con = duckdb.connect()

def main():
    species = search_species('saccharo')
    print(species)
    read_core_repeat_feature(species)
    read_core_seq_region(species)
    print(con.execute('DESCRIBE repeat_feature').fetchall())
    print(con.execute('''SELECT * from repeat_feature rf inner join seq_region sr
                      on sr.seq_region_id = rf.seq_region_id''').fetchone())

# Search for species by substring match
def search_species(search):
    read_ens_species()
    sql = f"SELECT species from species where species like '%{search}%'"
    res = con.execute(sql).fetchone()
    return f"{res[0]}"

# Generic table reader for core DB tables
def read_core_table(species, table, cols):
    sql = f"SELECT core_db from species where species = '{species}'"
    res = con.execute(sql).fetchone()
    if res == None:
        raise(Exception("Species not found"))
    # Can fetch directly from FTP
    # path = f"https://ftp.ensembl.org/pub/release-110/mysql/{res[0]}/{table}.txt.gz"
    path = f"data/{res[0]}/{table}.txt.gz"
    read_table(table, path, cols)

# Generic table reader
def read_table(name, file, cols):
    sql = f"CREATE TABLE {name} AS SELECT * FROM " + \
    f"read_csv('{file}'," + \
    f"header=true, delim='\t', columns={cols})"
    con.execute(sql)

# Core meta table
def read_core_meta(species):
    cols={'meta_id': 'INT', 'dummy': 'VARCHAR', 'meta_name': 'VARCHAR', 'meta_value': 'VARCHAR'}
    read_core_table(species, 'meta', cols)

# Core seq_region table
def read_core_seq_region(species):
    cols={
        'seq_region_id': 'INT',
        'name': 'VARCHAR',
        'coord_system_id': 'INT',
        'length': 'INT',
    }
    read_core_table(species, 'seq_region', cols)

# Core repeat_feature table
def read_core_repeat_feature(species):
    cols = {
        'repeat_feature_id': 'INT',
        'seq_region_id': 'INT',
        'seq_region_start': 'INT',
        'seq_region_end': 'INT',
        'seq_region_strand': 'INT',
        'repeat_start': 'INT',
        'repeat_end': 'INT',
        'repeat_consensus_id': 'INT',
        'analysis_id': 'INT',
        'score': 'DOUBLE',
    }
    read_core_table(species, 'repeat_feature', cols)

# Reads ensembl (mainly vertebrates) species metadata
def read_ens_species():
    cols = {'name': 'VARCHAR', 'species': 'VARCHAR', 'division': 'VARCHAR', 'taxonomy_id':
        'INT', 'assembly': 'VARCHAR', 'assembly_accession': 'VARCHAR',
        'genebuild': 'VARCHAR', 'variation': 'VARCHAR', 'microarray': 'VARCHAR',
        'pan_compara': 'VARCHAR', 'peptide_compara': 'VARCHAR', 'genome_alignments':
        'VARCHAR', 'other_alignments': 'VARCHAR', 'core_db': 'VARCHAR', 'species_id'
        : 'INT'}
    read_table('species', 'data/species_EnsemblVertebrates.txt', cols)

# Reads ensemblgenomes (everything not vertebrate) species metadata
def read_eg_species():
    cols = {'name': 'VARCHAR', 'species': 'VARCHAR', 'division': 'VARCHAR', 'taxonomy_id':
        'INT', 'assembly': 'VARCHAR', 'assembly_accession': 'VARCHAR',
        'genebuild': 'VARCHAR', 'variation': 'VARCHAR', 'microarray': 'VARCHAR',
        'pan_compara': 'VARCHAR', 'peptide_compara': 'VARCHAR', 'genome_alignments':
        'VARCHAR', 'other_alignments': 'VARCHAR', 'core_db': 'VARCHAR', 'species_id'
        : 'INT'}
    sql = """CREATE TABLE species AS SELECT * FROM read_csv('data/eg/species.txt',
    header=true, delim= '\t', columns=""" + f"{cols}" + ")"
    con.execute(sql)

main()
