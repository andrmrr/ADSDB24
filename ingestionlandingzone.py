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
  for ds in datasets:
    shutil.copyfile(ds, os.path.join(temporal_landing, os.path.basename(ds)))
  return len(datasets)

def load_to_persistent():
  if not os.path.isdir(persistent_landing):
    os.makedirs(persistent_landing)
  """Load to peristent landing"""
  if not os.path.isdir(temporal_landing):
    return
  dir_list = os.listdir(temporal_landing)
  curr_date = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
  for f in dir_list:
    fname, fext = f.split(".")[:-1], f.split(".")[-1]
    fname = ".".join(fname)
    if not os.path.isdir(os.path.join(persistent_landing, dataset_names[f])):
      os.makedirs(os.path.join(persistent_landing, dataset_names[f]))
    shutil.copyfile(os.path.join(temporal_landing, f), os.path.join(persistent_landing, dataset_names[f], fname + "_" + str(curr_date) + "." + fext))

  # clear temporal landing
  if os.path.isdir(temporal_landing):
    for f in os.listdir(temporal_landing):
      os.remove(os.path.join(temporal_landing, f))

  return len(dir_list)
