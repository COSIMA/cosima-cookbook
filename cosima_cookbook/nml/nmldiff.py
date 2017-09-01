#!/usr/bin/env python

import f90nml # from http://f90nml.readthedocs.io/en/latest/

def superset(nmlall):
    """Return dict of the groups/group members present in any of the input Namelists.
    
    Input: dict with key:value pairs where
            key is arbitrary (typically a filename string)
            value is Namelist (typically from filename)
    Output: dict with key:value pairs where
        key is group name (including all groups present in any input Namelist) 
        value is Namelist for group (with anything common to all other files removed)
    """
    nmlsuperset={}
    for nml in nmlall:
        nmlsuperset.update(nmlall[nml])
    # nmlsuperset now contains all groups that were in any nml
    for group in nmlsuperset:
        nmlsuperset[group]=nmlsuperset[group].copy() # to avoid the next bit changing the original groups
        for nml in nmlall:
            if group in nmlall[nml]:
                nmlsuperset[group].update(nmlall[nml][group])
    # nmlsuperset groups now contain all keys that were in any nml
    return nmlsuperset

def nmldiff(nmlfnames):
    """Return dict of the groups/group members that differ across multiple FORTRAN namelist files.
    
    Input: tuple of any number of namelist filename strings
    Output: dict with key:value pairs where
            key is filename strings 
            value is Namelist from filename, with anything common to all other files removed
    """

    nmlfnames=set(nmlfnames) # remove any duplicates from nmlfnames

    nmlall={} # dict keys are nml paths, values are Namelist dicts
    for nml in nmlfnames:
        nmlall[nml]=f90nml.read(nml)

# Create diff by removing common groups/members from nmlall. 
# This is complicated by the fact group names / member names may differ
# or be absent across different nml files.

# First make a superset that has all group names and group members that appear in any nml file
    nmlsuperset=superset(nmlall)
    
    # now go through nmlall and remove any groups / members from nmlall that are identical to superset in all nmls
    # first delete any group members that are common to all nmls, then delete any empty groups common to all nmls
    for group in nmlsuperset:
        deletegroup = True # whether group is present and identical in all namelist files
        for nml in nmlfnames:
            deletegroup = deletegroup and (group in nmlall[nml])
        if deletegroup: # group present in all namelist files
            for mem in nmlsuperset[group]:
                deletemem = True # whether group member is present and identical in all namelist files
                for nml in nmlfnames:
                    deletemem = deletemem and (mem in nmlall[nml][group])
                if deletemem: # group member is present in all namelist files
                    for nml in nmlfnames: # ... now check if values match in all namelist files
                        deletemem = deletemem and (nmlall[nml][group][mem] == nmlsuperset[group][mem])
                    if deletemem:
                        for nml in nmlfnames: # delete mem from this group in all nmls
                            del nmlall[nml][group][mem]
            for nml in nmlfnames:
                deletegroup = deletegroup and (len(nmlall[nml][group])==0)
            if deletegroup: # group is common to all nmls and now empty so delete
                for nml in nmlfnames:
                    del nmlall[nml][group]
    return nmlall
            