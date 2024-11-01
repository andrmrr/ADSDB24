import os
import shutil
import duckdb

from util import *
from ingestionlandingzone import *
from formattedzone import *
from trustedzone import *
from exploitationzone import *


def data_ingester():
    n = ingest_to_temporal()
    return "Ingested " + str(n) + " files into temporal landing"

def persistent_loader():
    n = load_to_persistent()
    return "Loaded " + str(n) + " files into persistent landing"

def formatted_loader():
    load_to_formatted()
    con = duckdb.connect(duckdb_formatted)
    tables = con.execute("SHOW TABLES").fetchall()
    ret_str = "Tables in the formatted database:\n"
    for table in tables:
        ret_str += table[0] + "\n"
    con.close()
    return ret_str

def trusted_loader():
    load_to_trusted()
    con = duckdb.connect(duckdb_trusted)
    tables = con.execute("SHOW TABLES").fetchall()
    ret_str = "Tables in the trusted database:\n"
    for table in tables:
        ret_str += table[0] + "\n"
    con.close()
    return ret_str

def exploitation_loader():
    load_to_exploitation()
    con = duckdb.connect(duckdb_exploitation)
    tables = con.execute("SHOW TABLES").fetchall()
    ret_str = "Tables in the exploitation database:\n"
    for table in tables:
        ret_str += table[0] + "\n"
    con.close()
    return ret_str

if __name__ == "__main__":
    """ingest data into temporal landing"""
    # ingest_to_temporal()
    load_to_persistent()

    """load into database in formatted zone"""
    load_to_formatted()
    """Check tables that are currently in the database"""
    con = duckdb.connect(duckdb_formatted)
    tables = con.execute("SHOW TABLES").fetchall()
    print("Tables in the formatted database:")
    for table in tables:
        print(table[0])
    con.close()

    """Load to trusted"""
    load_to_trusted()
    """Check tables that are currently in the database"""
    con = duckdb.connect(duckdb_trusted)
    tables = con.execute("SHOW TABLES").fetchall()
    print("Tables in the trusted database:")
    for table in tables:
        print(table[0])
    con.close()

    """Load to exploitation"""
    load_to_exploitation()
    """Check tables that are currently in the database"""
    con = duckdb.connect(duckdb_exploitation)
    tables = con.execute("SHOW TABLES").fetchall()
    print("Tables in the exploitation database:")
    for table in tables:
        print(table[0])
