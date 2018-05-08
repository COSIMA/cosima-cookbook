#!bin/sh
# In order use this command, it is necesary to move or create a vdiuser.config file in the home directory or 
# specify the path in the uconfigpath variable or replace "raijin_user" with the your account. 
# Additionally you could add the following line to your .bashrc file:
# source $COSIMA_COOKBOOK_PATH/connect_jvdi.sh > ~/.bashrc
# or you can run:
# echo "source $COSIMA_COOKBOOK_PATH/connect_jvdi.sh > ~/.bashrc"

uconfigpath=~/vdiuser.config

function get_session(){
    if [ -f ${uconfigpath} ]; then
	user="$(grep user ${uconfigpath} | cut -d ':' -f2 | tr -d " ',")" 
    else
	#Change the user in the following lines and comment the next line.
	echo Check the user or set up a vdiuser.config at your home directory.
        user="$(echo "ur4354")"
    fi
    echo -n "Determine if VDI session is already running..."
    running="$(ssh -o LogLevel=QUIET -t jm5970@vdi.nci.org.au "/opt/vdi/bin/session-ctl --configver=20151620513 list-avail --partition main")"
    if [ ! -z "$running" ];then
	echo "True"
	idnum="$(echo $running | sed -e 's/#~#id=\([0-9]*\).*/\1/')"
    else
        echo "False"
	echo -n  "Launching new VDI session..."
        running="$(ssh -o LogLevel=QUIET -t jm5970@vdi.nci.org.au "/opt/vdi/bin/session-ctl --configver=20151620513 launch --partition main")"
	idnum="$(echo $running | sed -e 's/#~#id=\([0-9]*\).*/\1/')"
    fi
    echo Determine jobid for VDI session... $idnum
    echo -n Host for VDI session...
    session="$(ssh -o LogLevel=QUIET -t jm5970@vdi.nci.org.au "/opt/vdi/bin/session-ctl --configver=20151620513 get-host --jobid ${idnum}")"
    sesionnnum="$(echo $session | sed -e 's/#~#host=vdi-n\([0-9]*\).*/\1/')"
    echo $sesionnnum
}

function connect_jvdi(){
    get_session
    ssh -Y -X $user@vdi-n$sesionnnum.nci.org.au
}

