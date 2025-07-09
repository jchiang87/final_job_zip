"""
Final job script to zip and rucio-register selected dataset types.
"""

import os
import sys
import glob
import tempfile
import subprocess
import yaml
import lsst.daf.butler as daf_butler
from lsst.pipe.base import QuantumGraph


def get_zip_file_locations(qgraph, dstypes):
    repo = qgraph.metadata['butler_argument']
    run_collection = qgraph.metadata['output_run']
    butler = daf_butler.Butler(repo, collections=[run_collection])
    zip_file_locations = {}
    for dstype in dstypes:
        ref = butler.query_datasets(dstype, limit=1)[0]
        zip_file_locations[dstype] = butler.getURI(ref).geturl().split("#")[0]
    return zip_file_locations


# Read in dataset types to zip.
zip_config_file = os.environ['ZIP_DSTYPE_CONFIG']
with open(zip_config_file) as fobj:
    zip_candidates = set(yaml.safe_load(fobj)['to_zip'])

qgraph_file = sys.argv[1]
butler_config = sys.argv[2]

# Get the dataset types of the QG output refs.
qgraph = QuantumGraph.loadUri(qgraph_file)
output_refs, _ = qgraph.get_refs(include_outputs=True,
                                 include_init_outputs=True,
                                 conform_outputs=True)
dstypes = set()
for ref in output_refs:
    dstypes.add(ref.datasetType.name)

# Make lists of dstypes to zip and not to zip.
to_zip = sorted(dstypes.intersection(zip_candidates))
not_to_zip = sorted(dstypes.difference(zip_candidates))

butler_exe = os.environ['DAF_BUTLER_DIR'] + "/bin/butler"

with tempfile.TemporaryDirectory() as zip_tmp_dir:
    # Zip the files for each dataset type individually.
    for dstype_to_zip in to_zip:
        zip_from_graph = [butler_exe,
                          "--long-log",
                          "--log-level=VERBOSE",
                          "zip-from-graph",
                          "--dataset-type",
                          dstype_to_zip,
                          qgraph_file,
                          butler_config,
                          zip_tmp_dir]
        subprocess.check_call(zip_from_graph)

    # Ingest the zip files.
    zip_files = glob.glob(f"{zip_tmp_dir}/*.zip")
    for zip_file in zip_files:
        ingest_zip = [butler_exe,
                      "--long-log",
                      "--log-level=VERBOSE",
                      "ingest-zip",
                      "--transfer",
                      "move",
                      butler_config,
                      zip_file]
        subprocess.check_call(ingest_zip)

# Rucio register each zip file.
zip_file_locations = get_zip_file_locations(qgraph, to_zip)
for dstype, zip_file in zip_file_locations.items():
    rucio_register = ["rucio-register",
                      "zips",
                      "--log-level=VERBOSE",
                      f"--rucio-dataset",
                      dstype,
                      f"--zip-file",
                      zip_file]
    subprocess.check_call(rucio_register)

# Transfer the remaining dataset types directly to the destination repo.
transfer_from_graph = [butler_exe,
                       "--long-log",
                       "--log-level=VERBOSE",
                       "transfer-from-graph",
                       "--dataset-type",
                       ','.join(not_to_zip),
                       qgraph_file,
                       butler_config,
                       "--register-dataset-types",
                       "--update-output-chain"]
subprocess.check_call(transfer_from_graph)
