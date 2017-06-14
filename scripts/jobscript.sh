#!/bin/bash

#SBATCH --account=ocp
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH -J dask_cluster
#SBATCH --time=24:00:00

source activate dask_distributed

SUBMIT_DIR=.
cd $SUBMIT_DIR
schedfile="$SUBMIT_DIR/.dask_scheduler/dask_scheduler_file-0"
dask-scheduler --scheduler-file $schedfile &
dask-worker --scheduler-file $schedfile &
JUPYTER_RUNTIME_DIR=$HOME/.jupyter/runtime jupyter notebook --ip='*' --no-browser .
