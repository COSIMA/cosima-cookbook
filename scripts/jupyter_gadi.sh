#!/bin/bash

# Scott Wales 20190522

print_help() {
cat <<EOF
Run a Jupyter notebook on Gadi's compute nodes, presenting the interface in a
browser on the local machine
General Options:
    -h:         Print help
    -l:         Gadi username
    -L:         Gadi login node (default 'gadi.nci.org.au')
Queue Options:
    -q QUEUE:   Queue name
    -n NCPU:    Use NCPU cpus
    -m MEM:     Memory allocation (default 4*NCPU GB)
    -t TIME:    Walltime limit (default 1 hour)
    -J JOBFS:   Jobfs allocation (default 100 GB)
    -P PROJ:    Submit job under project PROJ
EOF
}

set -eu

# Internal defaults
USER=''          # Add your nci username here
PROJECT='-P v45' # Note- should be the full pbs flag '-P a12' if overriding
LOGINNODE='gadi.nci.org.au'
QUEUE='express'  # QUEUE, NCPUS and MEM can be overridden in command line
NCPUS='8'
MEM='32gb'
WALLTIME=1:00:00
JOBFS=100gb

# Handle arguments
optspec="hl:L:q:n:m:t:J:P:"
while getopts "$optspec" optchar; do
    case "${optchar}" in
        h)
            print_help
            exit 2
            ;;
        l)
            USER="${OPTARG}"
            ;;
        L)
            LOGINNODE="${OPTARG}"
            ;;
        q)
            QUEUE="${OPTARG}"
            ;;
        n)
            NCPUS="${OPTARG}"
            ;;
        m)
            MEM="${OPTARG}"
            ;;
        t)
            WALLTIME="${OPTARG}"
            ;;
        J)
            JOBFS="${OPTARG}"
            ;;
        P)
            PROJECT="-P '${OPTARG}'"
            ;;
        *)
            print_help
            exit 2
            ;;
    esac
done

# This gets evaluated on Gadi in the SSH script
WORKDIR=\$TMPDIR/runjp

SSH='ssh -oBatchMode=yes'
if [ -n "$USER" ]; then
    SSH="${SSH} -l ${USER}"
fi
if [ -z "$MEM" ]; then
    MEM="$(( NCPUS * 4 ))gb"
fi

SUBMITOPTS="-N jupyter-notebook $PROJECT -q '$QUEUE' -l 'ncpus=${NCPUS},mem=${MEM},walltime=${WALLTIME},jobfs=${JOBFS}'"

echo "Starting notebook on ${LOGINNODE}..."

# Check connection
$SSH "$LOGINNODE" true

echo "qsub ${SUBMITOPTS}"

# Kill the job if this top-level script is cancelled while the job is still in the queue
trap "{ echo 'Stopping queued job... (Ctrl-C will leave job in the queue)' ; $SSH \"$LOGINNODE\" <<< \"qdel \\\$(cat \\$WORKDIR/jobid)\" ; }" EXIT

message=$(
$SSH -q "$LOGINNODE" <<EOF | tail -n 1
set -eu
WORKDIR="$WORKDIR"
mkdir -p "\$WORKDIR"
rm -f "\$WORKDIR/message"
qsub $SUBMITOPTS -j oe -o "\$WORKDIR/pbs.log" > \$WORKDIR/jobid <<EOQ
set -eu
# Jupyter security token
TOKEN=\\\$(uuidgen)
# Write message file with info for the local connection
echo "\\\$HOSTNAME \\\$TOKEN \\\$PBS_JOBID" > "\$WORKDIR/message"
echo "runjp log dir \$WORKDIR"
cat "\$WORKDIR/message"
module purge
module use /g/data3/hh5/public/modules
module load pbs
module load conda/analysis3-unstable
export DASK_LABEXTENSION__FACTORY__MODULE='dask_jobqueue'
export DASK_LABEXTENSION__FACTORY__CLASS='PBSCluster'
export DASK_LABEXTENSION__FACTORY__KWARGS__QUEUE='normal'
export DASK_LABEXTENSION__FACTORY__KWARGS__CORES='8'
export DASK_LABEXTENSION__FACTORY__KWARGS__MEMORY='16gb'
export DASK_LABEXTENSION__FACTORY__KWARGS__RESOURCE_SPEC='ncpus=8,mem=16gb'
export DASK_LABEXTENSION__FACTORY__KWARGS__INTERFACE='ib0'
jupyter notebook --NotebookApp.token="\\\$TOKEN" --no-browser --ip="\\\$HOSTNAME" --port 8888
EOQ
# Wait for the message file to appear, then return to the local process
while [ ! -f "\$WORKDIR/message" ]; do
    sleep 5
done
cat "\$WORKDIR/message"
EOF
)

echo "Remote Message: '$message'"

# Grab info from the PBS job
read jobhost token jobid <<< "$message"

# Find a local port
for local_port in {8888..9000}; do
    if ! echo > /dev/tcp/127.0.0.1/${local_port} ; then
        break
    fi 2> /dev/null
done

echo "Notebook running as PBS job ${jobid}"
echo
echo "Starting tunnel..."
$SSH -N -L "${local_port}:$jobhost:8888" -L "8787:$jobhost:8787" "$LOGINNODE" &
tunnelid=$!

# Shut everything down on exit
trap "{ echo 'Closing connections... (Ctrl-C will leave job in the queue)' ; kill $tunnelid ; $SSH "$LOGINNODE" qdel $jobid ; }" EXIT

# Wait for startup then open a browser
sleep 5
URL="http://localhost:${local_port}/lab?token=${token}"

echo
echo "Opening ${URL}"
if [ "$(uname)" = "Darwin" ]; then
    open "$URL"
else
    xdg-open "$URL"
fi

# Keep open as long as the tunnel exists
wait "$tunnelid"