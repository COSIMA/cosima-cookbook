from datetime import datetime
import sqlite3

import pandas as pd

from bokeh.io import curdoc
from bokeh.layouts import column
from bokeh.models import HoverTool
from bokeh.models.widgets import Select, Button
import bokeh.palettes
from bokeh.plotting import figure
from bokeh.transform import factor_cmap

dbname = '/g/data/v45/ahg157/cosima/cookbook.db'

expt_query = 'SELECT DISTINCT experiment FROM ncfiles'
vars_query = '''SELECT DISTINCT ncvars.variable FROM ncvars
                LEFT JOIN ncfiles ON ncvars.ncfile = ncfiles.id
                WHERE ncfiles.experiment = ?'''
data_query = '''SELECT ncfiles.ncfile, run, variable, time_start, time_end, frequency FROM ncfiles
                LEFT JOIN ncvars ON ncfiles.id = ncvars.ncfile
                WHERE ncfiles.experiment = ? AND time_start NOT NULL AND frequency <> 'static'
                ORDER BY variable, time_start'''

def get_data(expt):
    data = conn.execute(data_query, (expt,)).fetchall()
    df = pd.DataFrame(data, columns=['ncfile', 'run', 'variable', 'time_start', 'time_end', 'frequency'])
    df[['time_start', 'time_end']] = df[['time_start', 'time_end']].applymap(
        lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'))

    return df

conn = sqlite3.connect(dbname)
expts = [e[0] for e in conn.execute(expt_query)]

# create widgets
expt_select = Select(title='Experiment:', options=expts, value='1deg_jra55v13_iaf_spinup1_A')
refresh = Button(label='Update')

# hover tools
hover = HoverTool(tooltips=[
    ('variable', '@variable'), ('start', '@time_start{%F}'),
    ('end', '@time_end{%F}'), ('run', '@run'), ('file', '@ncfile')],
                  formatters={
                      'time_start': 'datetime',
                      'time_end': 'datetime'
                      })
tools = [hover, 'box_select', 'pan', 'box_zoom', 'wheel_zoom', 'reset']

df = get_data(expt_select.value)
freqs = df.frequency.unique()
cmap = factor_cmap('frequency', palette=bokeh.palettes.Category10[10], factors=freqs)

p = figure(y_range=df.variable.unique(), x_range=(df.iloc[0].time_start, df.iloc[-1].time_end),
           title=expt_select.value, tools=tools)
cmap = factor_cmap('frequency', palette=bokeh.palettes.Category10[10], factors=freqs)
hb = p.hbar(y='variable', left='time_start', right='time_end', height=0.4, source=df,
            fill_color=cmap, legend='frequency')

# callback routines to repopulate list of variables
def get_vars(expt):
    return [e[0] for e in conn.execute(vars_query, (expt,))]

def refresh_output():
    # get new data
    df = get_data(expt_select.value)
    freqs = df.frequency.unique()
    cmap = factor_cmap('frequency', palette=bokeh.palettes.Category10[10], factors=freqs)

    # update figure itself
    p.y_range.factors = list(df.variable.unique())
    (p.x_range.start, p.x_range.end) = (df.iloc[0].time_start, df.iloc[-1].time_end)
    p.title.text = expt_select.value

    # update data source for plot
    hb.data_source.data = hb.data_source.from_df(df)
    # update colourmap if necessary
    hb.glyph.fill_color = cmap
    
refresh.on_click(refresh_output)

# layout and show
layout = column(expt_select, refresh, p)
curdoc().add_root(layout)
