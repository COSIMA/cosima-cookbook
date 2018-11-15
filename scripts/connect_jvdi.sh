#!bin/sh
# To use this command, it is necesary to move or create a vdiuser.config file at your home directory,
# specify the path in the uconfigpath variable or replace "ur4354" with your raijin user name.
# Additionally, It's recomended to add the following line to your .bashrc file:
# source $COSIMA_COOKBOOK_PATH/scripts/connect_jvdi.sh > ~/.bashrc
# or you can run:
# echo "source $COSIMA_COOKBOOK_PATH/scripts/connect_jvdi.sh > ~/.bashrc"
# where $COSIMA_COOKBOOK_PATH is the path to the cosima cookbook directory.
# To run just type in the command line "connect_jvdi" after loading the functions. 

uconfigpath=~/cosima_cookbook.conf

function get_session(){
    if [ -f ${uconfigpath} ]; then
	user="$(grep user ${uconfigpath} | cut -d '=' -f2 | tr -d ' ')" 
    else
	user=
        echo "[DEFAULT]" > ${uconfigpath}
	while [[ $user = "" ]]; do
	    echo 'What is your Raijin username? '
	    read user
	done
	echo "user = " $user >> ${uconfigpath}
	echo jupyterport = 8889 >> ${uconfigpath}
        echo bokehport = 8787 >> ${uconfigpath}
        echo exechost = vdi.nci.org.au >> ${uconfigpath}
    fi
    pingin="$(ssh -o LogLevel=QUIET -t $user@vdi.nci.org.au "/opt/vdi/bin/session-ctl --configver=20151620513 list-avail --partition main")"
    if [ ! -z "$pingin" ];then
        echo -n "Determine if VDI session is already running..."
        running="$(ssh -o LogLevel=QUIET -t $user@vdi.nci.org.au "/opt/vdi/bin/session-ctl --configver=20151620513 list-avail --partition main")"
        if [ ! -z "$running" ];then
	    echo "True"
	    idnum="$(echo $running | sed -e 's/#~#id=\([0-9]*\).*/\1/')"
        else
            echo "False"
 	    echo -n  "Launching new VDI session..."
            running="$(ssh -o LogLevel=QUIET -t $user@vdi.nci.org.au "/opt/vdi/bin/session-ctl --configver=20151620513 launch --partition main")"
    	    idnum="$(echo $running | sed -e 's/#~#id=\([0-9]*\).*/\1/')"
        fi
        echo Determine jobid for VDI session... $idnum
        echo -n Host for VDI session...
        session="$(ssh -o LogLevel=QUIET -t $user@vdi.nci.org.au "/opt/vdi/bin/session-ctl --configver=20151620513 get-host --jobid ${idnum}")"
        sesionnnum="$(echo $session | sed -e 's/#~#host=vdi-n\([0-9]*\).*/\1/')"
        echo $sesionnnum
    else
	 echo "Incorrect user name in ~/cosima_cookbook.conf file?"
         echo "Edit ~/cosima_cookbook.conf before continuing."
    fi
}

function connect_jvdi(){
    get_session
    ssh -Y -X $user@vdi-n$sesionnnum.nci.org.au
}

