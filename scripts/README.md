## Usefull scripts


### `jupyter_raijin.sh`
Run a Jupyter notebook on Raijin's computer nodes, presenting the interface in a
browser on the local machine.

Usage:

```
jupyter_raijin.sh  -l nciusername -q queuename -n ncpus -m memory -P project
```

example:
```
jupyter_raijin.sh  -l nc1234 -q expressbw -n 10 -m 64gb -P v45
```
