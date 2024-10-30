import os
import shutil
import duckdb

from util import *
from ingestionlandingzone import *
from formattedzone import *
from trustedzone import *


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
