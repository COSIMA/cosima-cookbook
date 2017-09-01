#!/usr/bin/env python

import nmldiff
from IPython.display import display, Markdown
import os

def summary_md(configuration, expts, path = '/g/data3/hh5/tmp/cosima/',\
               search='https://github.com/OceansAus/access-om2/search?&q=',\
    nmls = [
        'atmosphere/input_atm.nml',
        'ice/cice_in.nml',
        'ice/input_ice.nml',
        'ice/input_ice_gfdl.nml',
        'ice/input_ice_monin.nml',
        'ocean/input.nml'
        ]):
    for nml in nmls:
        epaths = []
        mdstr = '| group | variable | '
        for e in expts:
            mdstr = mdstr + e + ' | '
            epaths.append(os.path.join(path,configuration,e,'output000',nml))
        nmld = nmldiff.nmldiff(tuple(epaths))
        nmldss = nmldiff.superset(nmld)
        display(Markdown('### ' + nml + ' namelist differences' ))
        if len(nmldss)==0:
            display(Markdown('no differences' ))
        else:
            mdstr = mdstr + '\n|---|:--|' + ':-:|'*len(expts)
            for group in sorted(nmldss):
                for mem in sorted(nmldss[group]):
                    mdstr = mdstr + '\n| ' + '&' + \
                    '[' + group + '](' + search + group + ')' + ' | ' + \
                    '[' + mem   + '](' + search + mem   + ')' + ' | '
                    for e in epaths:
                        if group in nmld[e]:
                            if mem in nmld[e][group]:
                                mdstr = mdstr + repr(nmld[e][group][mem])
                        mdstr = mdstr + ' | '
            display(Markdown(mdstr))
    return
            