import os
import shutil
import duckdb

from util import *
from ingestionlandingzone import *
from formattedzone import *
from trustedzone import *
from exploitationzone import *
from model_training import *
from sandbox import *
from data_preparation import *

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
    load_trusted()
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

def sandbox():
    load_sandbox()
    con = duckdb.connect(duckdb_sandbox)
    tables = con.execute("SHOW TABLES").fetchall()
    ret_san = "Tables in the analytical sandbox:\n"
    for table in tables:
        ret_san += table[0] + "\n"
    con.close()
    return ret_san

def feature_enginering():
    data_preparation_execute()
    con = duckdb.connect(duckdb_sandbox)
    tables = con.execute("SHOW TABLES").fetchall()
    ret_fe = "Tables after feature engineering:\n"
    for table in tables:
        ret_fe += table[0] + "\n"
    con.close()
    return ret_fe

def model_training_loader():
    model_training()
    mse = mse_test
    r2 = r2_test

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
    load_trusted()
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

    """Load to analytical sandbox"""
    load_sandbox()
    """Check tables that are currently in the database"""
    con = duckdb.connect(duckdb_sandbox)
    tables = con.execute("SHOW TABLES").fetchall()
    print("Tables in the sandbox database:")
    for table in tables:
        print(table[0])

    """Feature engineering zone"""
    data_preparation_execute()
    """Check tables that are currently in the database"""
    con = duckdb.connect(duckdb_sandbox)
    tables = con.execute("SHOW TABLES").fetchall()
    print("Tables after feature engineering:")
    for table in tables:
        print(table[0])
    
    """Check tables that are currently in the database"""
    con = duckdb.connect(duckdb_exploitation)
    tables = con.execute("SHOW TABLES").fetchall()
    print("Tables in the exploitation database:")
    for table in tables:
        print(table[0])
