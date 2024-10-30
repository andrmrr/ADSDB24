"""
Landing zone

"""
import os
import shutil
import datetime
from util import *

"""Load to temporal landing"""
def ingest_to_temporal():
  if not os.path.isdir(temporal_landing):
    os.makedirs(temporal_landing)
  else:
    for f in os.listdir(temporal_landing):
      os.remove(os.path.join(temporal_landing, f))
  for ds in datasources:
    shutil.copyfile(ds, os.path.join(temporal_landing, os.path.basename(ds)))

def load_to_persistent():
  """Load to peristent landing"""
  dir_list = os.listdir(temporal_landing)
  curr_date = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
  for f in dir_list:
    fname, fext = f.split(".")[:-1], f.split(".")[-1]
    fname = ".".join(fname)
    if not os.path.isdir(os.path.join(persistent_landing, datasource_names[f])):
      os.makedirs(os.path.join(persistent_landing, datasource_names[f]))
    shutil.copyfile(os.path.join(temporal_landing, f), os.path.join(persistent_landing, datasource_names[f], fname + "_" + str(curr_date) + "." + fext))
