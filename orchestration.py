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
from feature_engineering import *
from model_training import *

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

def sandbox_loader():
    load_sandbox()
    con = duckdb.connect(duckdb_sandbox)
    tables = con.execute("SHOW TABLES").fetchall()
    ret_san = "Tables in the analytical sandbox:\n"
    for table in tables:
        ret_san += table[0] + "\n"
    con.close()
    return ret_san

def feature_enginering_loader():
    feature_engineering_execute()
    con = duckdb.connect(duckdb_sandbox)
    tables = con.execute("SHOW TABLES").fetchall()
    ret_fe = "Tables after feature engineering:\n"
    for table in tables:
        ret_fe += table[0] + "\n"
    con.close()
    return ret_fe

def data_preparation_loader():
    data_preparation_execute()
    con = duckdb.connect(duckdb_sandbox)
    tables = con.execute("SHOW TABLES").fetchall()
    ret_dp = "Tables after data preparation:\n"
    for table in tables:
        ret_dp += table[0] + "\n"
    con.close()
    return ret_dp

def model_training_loader():
    mse, r2, prediction = model_training_execute()
    
    model_text = f"MSE on test set: {mse}" + "\n"
    model_text += f"R2 on test set: {r2}" + "\n"
    model_text += f"Model predictions: " + "\n"
    for i, prediction in enumerate(prediction):
        model_text += f"Sample {i + 1}: {prediction}"
    return model_text

if __name__ == "__main__":
    """ingest data into temporal landing"""
    ingest_to_temporal()
    load_to_persistent()

    """load into database in formatted zone"""
    re_for = formatted_loader()

    """Load to trusted"""
    re_tr = trusted_loader()

    con_ex = duckdb.connect(os.path.join(base_dir, duckdb_trusted))
    tables = con_ex.execute("SHOW TABLES").fetchall()
    for table in tables:
        print("Tru " + table[0])
    con_ex.close()

    """Load to exploitation"""
    #re_exp = exploitation_loader()

    # con_ex = duckdb.connect(os.path.join(base_dir, duckdb_exploitation))
    # tables = con_ex.execute("SHOW TABLES").fetchall()
    # for table in tables:
    #     print("Exp " + table[0])
    # con_ex.close()

    """Load to analytical sandbox"""
    # re_san = sandbox_loader()
    # print(re_san)

    # con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))
    # tables1 = con_sb.execute("SHOW TABLES").fetchall()
    # for table in tables1:
    #     print("Sandbox " + table[0])
    # con_sb.close()

    """Feature engineering zone"""
    #re_fe = feature_engineering_execute()

    """Data preparation"""
    #re_dp = data_preparation_execute()

    """Model training"""
    #model_text = model_training_execute()
    

