#!/usr/bin/env python

# Tools to read a set of namelist files and find their superset and difference.
# The functions are general-purpose (i.e. no ACCESS-OM2-related assumptions).
# Andrew Kiss https://github.com/aekiss


import f90nml  # from http://f90nml.readthedocs.io/en/latest/
import os


def nmldict(nmlfnames):
    """Return dict of the groups/group members of multiple
        FORTRAN namelist files.

    Input: tuple of any number of namelist file path strings
            (non-existent files are silently ignored)
    Output: dict with key:value pairs where
            key is filename path string
            value is complete Namelist from filename
    """
    nmlfnames = set(nmlfnames)  # remove any duplicates from nmlfnames

    nmlall = {}  # dict keys are nml paths, values are Namelist dicts
    for nml in nmlfnames:
        if os.path.exists(nml):
            nmlall[nml] = f90nml.read(nml)
    return nmlall


def superset(nmlall):
    """Return dict of groups/group members present in any of the input Namelists.

    Input: dict with key:value pairs where
            key is arbitrary (typically a filename string)
            value is Namelist (typically from filename)
    Output: dict with key:value pairs where
        key is group name (including all groups present in any input Namelist)
        value is Namelist for group (with nothing common to all other files)
    """
    nmlsuperset = {}
    for nml in nmlall:
        nmlsuperset.update(nmlall[nml])
    # nmlsuperset now contains all groups that were in any nml
    for group in nmlsuperset:
        # to avoid the next bit changing the original groups
        nmlsuperset[group] = nmlsuperset[group].copy()
        for nml in nmlall:
            if group in nmlall[nml]:
                nmlsuperset[group].update(nmlall[nml][group])
    # nmlsuperset groups now contain all keys that were in any nml
    return nmlsuperset


def nmldiff(nmlall):
    """Remove every group/group member that is the same in all file Namelists.

    Parameter
    ---------
    Input : dict
        (e.g. returned by nmldict) with key:value pairs where
        key is filename path string
        value is complete Namelist from filename
    Output : dict
        modified input dict with key:value pairs where
        key is filename strings
        value is Namelist from filename, with any group/group member
        common to all other files removed
    """

    # Create diff by removing common groups/members from nmlall.
    # This is complicated by the fact group names / member names may differ
    # or be absent across different nml files.

    # First make a superset that has all group names and group members that
    # appear in any nml file
    nmlsuperset = superset(nmlall)

    # now go through nmlall and remove any groups / members from nmlall that
    #   are identical to superset in all nmls
    # first delete any group members that are common to all nmls, then delete
    #   any empty groups common to all nmls
    for group in nmlsuperset:
        # init: whether group is present and identical in all namelist files
        deletegroup = True
        for nml in nmlall:
            deletegroup = deletegroup and (group in nmlall[nml])
        if deletegroup:  # group present in all namelist files
            for mem in nmlsuperset[group]:
                # init: whether group member is present and identical
                #   in all namelist files
                deletemem = True
                for nml in nmlall:
                    deletemem = deletemem and (mem in nmlall[nml][group])
                if deletemem:  # group member is present in all namelist files
                    for nml in nmlall:
                        # ... now check if values match in all namelist files
                        deletemem = deletemem and (
                            nmlall[nml][group][mem] == nmlsuperset[group][mem]
                        )
                    if deletemem:
                        for nml in nmlall:
                            # delete mem from this group in all nmls
                            del nmlall[nml][group][mem]
            for nml in nmlall:
                deletegroup = deletegroup and (len(nmlall[nml][group]) == 0)
            if deletegroup:
                # group is common to all nmls and now empty so delete
                for nml in nmlall:
                    del nmlall[nml][group]
    return nmlall
