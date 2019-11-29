## Usefull scripts


 ### `jupyter_gadi.sh`
 Run a Jupyter notebook on Gadi's computer nodes, presenting the interface in a
 browser on your local machine.

 Usage:

 ```
 jupyter_raijin.sh  -l nciusername -q queuename -n ncpus -m memory -P project
 ```

 example:
 ```
 jupyter_gadi.sh  -l nc1234 -q express -n 10 -m 64gb -P v45
 ```