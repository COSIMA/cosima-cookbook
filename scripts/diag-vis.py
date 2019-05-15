from datetime import datetime
from sys import argv

import cosima_cookbook as cc
import pandas as pd
from sqlalchemy import select, distinct, bindparam

from bokeh.io import curdoc
from bokeh.layouts import column
from bokeh.models.callbacks import CustomJS
from bokeh.models.sources import ColumnDataSource
from bokeh.models.tools import BoxSelectTool, HoverTool, TapTool
from bokeh.models.widgets import Select, Button, Div
import bokeh.palettes
from bokeh.plotting import figure
from bokeh.transform import factor_cmap

if len(argv) < 2:
    raise Exception('Usage: bokeh serve diag-vis.py --args <db>')
db = argv[1]

conn, tables = cc.database.create_database(db)

expt_query = select([distinct(tables['ncfiles'].c.experiment)])
vars_query = select([distinct(tables['ncvars'].c.variable)]) \
             .select_from(tables['ncvars'].join(tables['ncfiles'])) \
             .where(tables['ncfiles'].c.experiment == bindparam('expt'))
data_query = select([tables['ncfiles'].c.ncfile, tables['ncfiles'].c.run, tables['ncvars'].c.variable,
                     tables['ncfiles'].c.time_start, tables['ncfiles'].c.time_end, tables['ncfiles'].c.frequency]) \
             .select_from(tables['ncfiles'].join(tables['ncvars'])) \
             .where(tables['ncfiles'].c.experiment == bindparam('expt')) \
             .where(tables['ncfiles'].c.time_start is not None) \
             .where(tables['ncfiles'].c.frequency != 'static') \
             .order_by(tables['ncvars'].c.variable, tables['ncfiles'].c.time_start)

expts = [e[0] for e in conn.execute(expt_query)]

def get_data(expt):
    data = conn.execute(data_query, expt=expt).fetchall()
    df = pd.DataFrame(data, columns=['ncfile', 'run', 'variable', 'time_start', 'time_end', 'frequency'])
    df[['time_start', 'time_end']] = df[['time_start', 'time_end']].applymap(
        lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'))

    return df

def print_selected(div):
    return CustomJS(args=dict(div=div), code="""
var source = cb_obj;
var unique_vars = {};
for (var i of source.selected['1d'].indices) {
  var v = source.data['variable'][i];
  if (v in unique_vars) {
    unique_vars[v]['time_start'] = Math.min(unique_vars[v]['time_start'], source.data['time_start'][i]);
    unique_vars[v]['time_end'] = Math.max(unique_vars[v]['time_end'], source.data['time_end'][i]);
  } else {
    unique_vars[v] = { time_start: source.data['time_start'][i],
                       time_end: source.data['time_end'][i] };
  }
}

var text = '<table><tr><th>Name</th><th>Start</th><th>End<th></tr>';
for (var p in unique_vars) {
  var ts = new Date(unique_vars[p]['time_start']);
  var te = new Date(unique_vars[p]['time_end']);
  text = text.concat('<tr><th>'+p+'</th><td>'+ts.toISOString().substr(0,10)+'</td><td>'+te.toISOString().substr(0,10)+'</td></tr>');
}
text = text.concat('</table>')
div.text = text;
""")


# create widgets
expt_select = Select(title='Experiment:', options=expts, value=expts[0])
refresh = Button(label='Update')
div = Div(width=1000)

# hover tools
hover = HoverTool(tooltips=[
    ('variable', '@variable'), ('start', '@time_start{%F}'),
    ('end', '@time_end{%F}'), ('run', '@run'), ('file', '@ncfile')],
                  formatters={
                      'time_start': 'datetime',
                      'time_end': 'datetime'
                      })
tap = TapTool()
box_select = BoxSelectTool()
tools = [hover, box_select, tap, 'pan', 'box_zoom', 'wheel_zoom', 'reset']

df = get_data(expt_select.value)
freqs = df.frequency.unique()
cmap = factor_cmap('frequency', palette=bokeh.palettes.Category10[10], factors=freqs)
cds = ColumnDataSource(df, callback=print_selected(div))

p = figure(y_range=df.variable.unique(), x_range=(df.iloc[0].time_start, df.iloc[-1].time_end),
           title=expt_select.value, tools=tools)
cmap = factor_cmap('frequency', palette=bokeh.palettes.Category10[10], factors=freqs)
hb = p.hbar(y='variable', left='time_start', right='time_end', height=0.4, source=cds,
            fill_color=cmap, legend='frequency')

# callback routines to repopulate list of variables
def get_vars(expt):
    return [e[0] for e in conn.execute(vars_query, expt=expt)]

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
layout = column(expt_select, refresh, p, div)
curdoc().add_root(layout)
