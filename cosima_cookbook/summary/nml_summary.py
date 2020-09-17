#!/usr/bin/env python

# Create tabulated summary of namelists for a set of files.
# These functions assume we are dealing with ACCESS-OM2 data.
# Andrew Kiss https://github.com/aekiss


import cosima_cookbook as cc
from IPython.display import display, Markdown
import os


def summary_md(
    configuration,
    expts,
    path="/g/data3/hh5/tmp/cosima/",
    search="https://github.com/OceansAus/access-om2/search?&q=",
    nmls=[
        "atmosphere/input_atm.nml",
        "ice/cice_in.nml",
        "ice/input_ice.nml",
        "ice/input_ice_gfdl.nml",
        "ice/input_ice_monin.nml",
        "ocean/input.nml",
    ],
):
    for nml in nmls:
        epaths = []
        for e in expts:
            # NB: only look at output000
            epaths.append(os.path.join(path, configuration, e, "output000", nml))
        nmld = cc.nmldiff(cc.nmldict(tuple(epaths)))
        epaths = list(nmld.keys())  # redefine to handle missing paths
        epaths.sort()
        nmldss = cc.superset(nmld)
        display(Markdown("### " + nml + " namelist differences"))
        if len(nmldss) == 0:
            display(Markdown("no differences"))
        else:
            mdstr = "| group | variable | "
            for e in epaths:
                mdstr = mdstr + e.replace("/", "/<br>") + " | "
            mdstr = mdstr + "\n|---|:--|" + ":-:|" * len(epaths)
            for group in sorted(nmldss):
                for mem in sorted(nmldss[group]):
                    mdstr = mdstr + "\n| " + "&" + group + " | " + mem + " | "
                    #                        search doesn't work on github submodules or forks
                    #                        '[' + group + '](' + search + group + ')' + ' | ' + \
                    #                        '[' + mem + '](' + search + mem + ')' + ' | '
                    for e in epaths:
                        if group in nmld[e]:
                            if mem in nmld[e][group]:
                                mdstr = mdstr + repr(nmld[e][group][mem])
                        mdstr = mdstr + " | "
            display(Markdown(mdstr))
    return
