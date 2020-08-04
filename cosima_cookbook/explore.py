from collections import OrderedDict
import re

import cosima_cookbook as cc
import ipywidgets as widgets
from ipywidgets import HTML, Button, VBox, HBox, Label, Layout, Select
from ipywidgets import SelectMultiple, Tab, Text, Textarea, Checkbox
from ipywidgets import interact, interact_manual, AppLayout, Dropdown
import ipywidgets as wid

import pandas as pd

from cosima_cookbook.database import CFVariable, NCFile, NCExperiment, NCVar

from sqlalchemy import func

def return_value_or_empty(value):
    """Return value if not None, otherwise empty"""
    if value is None:
        return ''
    else:
        return value

class DatabaseExtension:

    session = None
    experiments = None
    keywords = None
    variables = None
    expt_variable_map = None
    
    def __init__(self, session=None, experiments=None):
        if session is None:
            session = cc.database.create_session()
        self.session = session

        self.allexperiments = cc.querying.get_experiments(session, all=True)

        if experiments is None:
            self.experiments = self.allexperiments
        else:
            if isinstance(experiments, str):
                experiments = [experiments,]
            # Subset experiment column from dataframe, and don't pass as a simple list
            # otherwise index is not correctly named
            self.experiments = self.allexperiments[self.allexperiments.experiment.isin(experiments)]

        self.keywords = sorted(cc.querying.get_keywords(session), key=str.casefold)
        self.expt_variable_map = self.experiment_variable_map()
        self.variables = self.unique_variable_list()

    def experiment_variable_map(self):
        """
        Make a pandas table with experiment as the index and columns
        of name, long_name and restart flag.

        Also make lists of unique name/long_name 
        """
        allvars = pd.concat([self.get_variables(expt) for expt in self.experiments.experiment], 
                            keys=self.experiments.experiment)

        # Create a new column to flag if variable is from a restart directory
        allvars['restart'] = allvars.ncfile.str.contains('restart')

        # Create a new column to characterise model type
        allvars['model'] = None

        # There is no metadata in the files or database that will let us know which
        # model produced the output, so use a heuristic that assumes if the data
        # resides in a directory that is named for a model type it is output from 
        # that model. Doesn't use os.path.sep as it is never envisaged this will be used 
        # outside of a posix system
        allvars.loc[(allvars.ncfile.str.contains('/ocean/')  |
                     allvars.ncfile.str.contains('/ocn/')), 'model'] = 'ocean'
        allvars.loc[(allvars.ncfile.str.contains('/atmosphere/') | 
                     allvars.ncfile.str.contains('/atm/')), 'model'] = 'atmosphere'
        allvars.loc[allvars.ncfile.str.contains('/ice/'), 'model'] = 'ice'

        allvars['model'] = allvars['model'].astype('category')

        # Create a new column to flag if variable has units which match a number of criteria
        # that indicated it is a coordinate
        allvars = allvars.assign(coordinate=(allvars.units.str.contains('degrees', na=False) |
                                             allvars.units.str.contains('since', na=False)   |
                                             allvars.units.str.match('^radians$', na=False)  |
                                             allvars.units.str.startswith('days', na=False)))  # legit units: %/day, day of year

        return allvars[['name', 'long_name', 'model', 'restart', 'coordinate']]

    def unique_variable_list(self):
        """
        Extract a list of all variable name/long_name pairs from the experiment
        keyword map
        """
        return self.expt_variable_map.reset_index(drop=True).drop_duplicates()
        
    def keyword_filter(self, keywords):
        """
        Return a list of experiments matching *all* of the supplied keywords
        """
        try:
            return cc.querying.get_experiments(self.session, keywords=keywords).experiment
        except AttributeError:
            return []

    def variable_filter(self, variables):
        """
        Return a set of experiments that contain all the defined variables
        """
        expts = []
        for v in variables:
            expts.append(
                set(self.expt_variable_map[self.expt_variable_map.name == v].reset_index()['experiment'])
            )
        return set.intersection(*expts)
    
    def get_experiment(self, experiment):
        return self.experiments[self.experiments['experiment'] == experiment]

    # Return more metadata than get_variables from cosima-cookbook
    def get_variables(self, experiment, frequency=None):
        """
        Returns a DataFrame of variables for a given experiment and optionally
        a given diagnostic frequency.
        """

        q = (self.session
            .query(CFVariable.name,
                    CFVariable.long_name,
                    CFVariable.standard_name,
                    CFVariable.units,
                    NCFile.frequency,
                    NCFile.ncfile,
                    func.count(NCFile.ncfile).label('# ncfiles'),
                    func.min(NCFile.time_start).label('time_start'),
                    func.max(NCFile.time_end).label('time_end'))
            .join(NCFile.experiment)
            .join(NCFile.ncvars)
            .join(NCVar.variable)
            .filter(NCExperiment.experiment == experiment)
            .order_by(NCFile.frequency,
                    CFVariable.name,
                    NCFile.time_start,
                    NCFile.ncfile)
            .group_by(CFVariable.name, NCFile.frequency))

        if frequency is not None:
            q = q.filter(NCFile.frequency == frequency)

        return pd.DataFrame(q)

class VariableSelector(VBox):
    """
    Combo widget based on a Select box with a search panel above to live
    filter variables to Select. When a variable is selected the long name
    attribute is displayed under the select box. There are also two 
    checkboxes which hide coordinates and restart variables.

    Note that a dict is used to populate the Select widget, so the visible
    value is the variable name and is accessed via the label attribute, 
    and the long name via the value attribute. 
    """

    variables = None
    widgets = {}

    def __init__(self, variables, rows=10, **kwargs):
        """
        variables is a pandas dataframe. kwargs are passed through to child
        widgets which, theoretically, allows for layout information to be
        specified
        """
        self._make_widgets(rows)
        super().__init__(children=list(self.widgets.values()), **kwargs)
        self.set_variables(variables)
        self._set_info()
        self._set_observes()

    def _make_widgets(self, rows):

        # Experiment selector element
        self.widgets['model'] = Dropdown(
            options=(),
            layout={'padding': '0px 5px', 'width': 'initial'},
            description='',
        )
        # Variable search
        self.widgets['search'] = Text(
            placeholder='Search: start typing', 
            layout={'padding': '0px 5px', 'width': 'auto', 'overflow-x': 'scroll'},
        )
        # Variable selection box
        self.widgets['selector'] = Select(
            options=(), #sorted(self.variables.name, key=str.casefold),
            rows=rows,
            layout=self.widgets['search'].layout
        )
        # Variable info
        self.widgets['info'] = HTML(
            layout=self.widgets['search'].layout
        )
        # Variable filtering elements
        self.widgets['filter_coords'] = Checkbox(
            value=True,
            indent=False,
            description='Hide coordinates',
        )
        self.widgets['filter_restarts'] = Checkbox(
            value=True,
            indent=False,
            description='Hide restarts',
        )

    def _set_observes(self):
        """
        Set event handlers
        """
        for w in ['filter_coords', 'filter_restarts']:
            self.widgets[w].observe(self._filter_eventhandler, names='value')

        self.widgets['model'].observe(self._model_eventhandler, names='value')
        self.widgets['search'].observe(self._search_eventhandler, names='value')
        self.widgets['selector'].observe(self._selector_eventhandler, names='value')

    def set_variables(self, variables):
        """
        Change variables
        """
        # Add a new column to keep track of visibility in widget
        self.variables = variables.assign(visible=True)

        # Set default filtering
        self._filter_variables()

        # Update selector
        self._update_selector(self.variables[self.variables.visible])

    def _update_selector(self, variables):
        """
        Update the variables in the selector. The variable are passed as an
        argument, so can differ from the internal variable list. This allows
        for easy filtering
        """
        # Populate model selector. Note label and value differ
        options = {'All models': ''}
        for model in variables.model.cat.categories.values:
            options["{} only".format(model.capitalize())] = model
        self.widgets['model'].options = options

        # Populate variable selector
        self.widgets['selector'].options = dict(variables.sort_values(['name'])[['name','long_name']].values)

    def _reset_filters(self):
        """
        Reset filters to default values
        """
        for w in ['filter_coords', 'filter_restarts']:
            self.widgets[w].value = True

    def _model_eventhandler(self, event=None):
        """
        Filter by model 
        """
        model = self.widgets['model'].value

        # Reset the coord and restart filters when a model changed
        self._reset_filters()
        self._filter_variables(model=model)

    def _filter_eventhandler(self, event=None):

        self._filter_variables(self.widgets['filter_coords'].value,
                               self.widgets['filter_restarts'].value,
                               self.widgets['model'].value)

    def _filter_variables(self, coords=True, restarts=True, model=''):
        """
        Optionally hide some variables
        """
        # Set up a mask with all true values
        mask = self.variables.name.ne('')

        # Filter for matching models
        if model != '':
            mask = mask & (self.variables['model'] == model)

        # Conditionally filter out restarts and coordinates
        if coords:
            mask = mask & ~self.variables['coordinate']
        if restarts:
            mask = mask & ~self.variables['restart']

        # Mask out hidden variables
        self.variables['visible'] = mask

        # Update the variable selector
        self._update_selector(self.variables[self.variables.visible])

        # Reset the search
        self.widgets['search'].value = ''
        self.widgets['selector'].value = None

    def _search_eventhandler(self, event=None):
        """
        Live search bar, updates the selector options dynamically, does not alter
        visible mask in variables
        """
        search_term = self.widgets['search'].value

        variables = self.variables[self.variables.visible]
        if search_term is not None or search_term != '':
            try:
                variables = variables[variables.name.str.contains(search_term, na=False) |
                                    variables.long_name.str.contains(search_term, na=False) ]
            except:
                print('Illegal character in search!')
                search_term = self.widgets['search'].value

        self._update_selector(variables)
    
    def _selector_eventhandler(self, event=None):
        """
        Update variable info when variable selected
        """
        self._set_info(self.widgets['selector'].value)
    
    def _set_info(self, long_name=None):
        """
        Set long name info widget 
        """
        if long_name is None or long_name == '':
            long_name = '&nbsp;'
        style = '<style>p{word-wrap: break-word}</style>' 
        self.widgets['info'].value = style + '<p>{long_name}</p>'.format(long_name=long_name)
    
    def delete(self, variable_names=None):
        """
        Remove variables
        """
        # If no variable specified just delete the currently selected one
        if variable_names is None:
            if self.widgets['selector'].label is None:
                return None
            else:
                variable_names = [ self.widgets['selector'].label, ]

        if isinstance(variable_names, str):
            variable_names = [ variable_names, ]

        mask = self.variables['name'].isin(variable_names)
        deleted = self.variables[mask]

        # Delete variables
        self.variables = self.variables[~mask]

        # Update selector. Use search eventhandler so the selector preserves any 
        # current search term. It is annoying to have that reset and type in again 
        # if multiple variables are to be added
        self._search_eventhandler()

        return deleted

    def add(self, variables):
        """
        Add variables
        """
        # Concatenate existing and new variables
        self.variables = pd.concat([self.variables, variables])

        # Need to recalculate the visible flag as new variables have been added
        self._filter_eventhandler(None)

    def get_selected(self):
        """
        Return currently selected variable name
        """
        return self.widgets['selector'].label

class VariableSelectorInfo(VariableSelector):
    """
    Subclass of VariableSelector to display more info in a separate widget
    """

    def __init__(self, variables, daterange, frequency, rows=10, **kwargs):

        super(VariableSelectorInfo, self).__init__(variables, rows, **kwargs)

        # Requires two widgets passed in as an argument. An html box where 
        # extended meta-data will be displayed, and a date range widget for
        # selecting start and end times to load
        self.widgets['daterange'] = daterange
        self.widgets['frequency'] = frequency

        self._filter_eventhandler(None)

        self.widgets['selector'].observe(self._var_eventhandler, names='value')
        self.widgets['frequency'].observe(self._frequency_eventhandler, names='value')

    def _var_eventhandler(self, selector):
        """
        Called when variable selected
        """
        variable_name = self.widgets['selector'].label
        variable = self.variables.loc[self.variables['name'] == variable_name]

        # Initialise daterange widget
        self.widgets['daterange'].options = ['0000','0000']
        self.widgets['daterange'].disabled = True

        self.widgets['frequency'].options = []
        self.widgets['frequency'].disabled = True

        if len(variable) == 0:
            return
        
        self.widgets['frequency'].options = variable.frequency
        self.widgets['frequency'].index = 0
        self.widgets['frequency'].disabled = False

    def _frequency_eventhandler(self, selector):

        variable_name = self.widgets['selector'].label
        frequency = self.widgets['frequency'].value

        variable = self.variables.loc[(self.variables['name'] == variable_name) & (self.variables['frequency'] == frequency)]

        try:
            # Populate daterange widget if variable contains necessary information
            # Convert human readable frequency to pandas compatigle frequency string 
            freq = re.sub(r'^(\d+) (\w)(\w+)', r'\1\2', str(variable.frequency.values[0]).upper())
            dates = pd.date_range(variable.time_start.values[0], variable.time_end.values[0] , freq=freq)
            self.widgets['daterange'].options = [(i.strftime('%Y/%m/%d'), i) for i in dates]                
            self.widgets['daterange'].value = (dates[0], dates[-1])
        except:
            pass
        finally:
            self.widgets['daterange'].disabled = False

class VariableSelectFilter(widgets.HBox):
    """
    Combo widget which contains a VariableSelector from which variables can 
    be transferred to another Select Widget to specify which variables should
    be used to filter experiments
    """

    variables = pd.DataFrame()
    widgets = {}
    subwidgets = {}
    buttons = {}

    def __init__(self, selvariables, **kwargs):
        """
        selvariables is a dataframe and is used to populate the VariableSelector

        self.variables contains the variables transferred to the selected widget
        """

        layout = {'padding': '0px 5px'}

        # Variable selector combo-widget
        self.widgets['selector'] = VariableSelector(selvariables, **kwargs)

        # Button to add variable from selector to selected
        self.buttons['var_filter_add'] = Button(
            tooltip='Add selected variable to filter',
            icon='angle-double-right',
            layout={'width': 'auto'},
        )
        # Button to add variable from selector to selected
        self.buttons['var_filter_sub'] = Button(
            tooltip='Remove selected variable from filter',
            icon='angle-double-left',
            layout={'width': 'auto'},
        )
        self.widgets['button_box'] = VBox(list(self.buttons.values()), 
                                          layout={'padding': '100px 5px', 'height': '100%'})

        # Selected variables for filtering with header widget
        self.subwidgets['var_filter_label'] = HTML('Filter variables:', layout=layout)
        self.subwidgets['var_filter_selected'] = Select(
            options=[],
            rows=10,
            layout=layout,
        )
        self.widgets['filter_box'] = VBox(list(self.subwidgets.values()), layout=layout)

        super().__init__(children=list(self.widgets.values()), **kwargs)

        self._set_observes()

    def _set_observes(self):
        """
        Set event handlers
        """
        self.buttons['var_filter_add'].on_click(self._add_var_to_selected)
        self.buttons['var_filter_sub'].on_click(self._sub_var_from_selected)

    def _update_variables(self):
        """
        Update filtered variables
        """
        self.subwidgets['var_filter_selected'].options = dict(self.variables.sort_values(['name'])[['name','long_name']].values)

    def _add_var_to_selected(self, button):
        """
        Transfer variable from selector to filtered variables
        """
        self.add(self.widgets['selector'].delete())

    def add(self, variable):
        """
        Add variable to filtered variables
        """
        if variable is None or len(variable) == 0:
            return
        self.variables = pd.concat([self.variables, variable])
        self._update_variables()

    def _sub_var_from_selected(self, button):
        """
        Transfer variable from filtered variables to selector
        """
        self.widgets['selector'].add(self.delete())

    def delete(self, variable_names=None):
        """
        Delete variable from filtered variables
        """
        # If no variable specified just delete the currently selected one
        if variable_names is None:
            if self.subwidgets['var_filter_selected'].label is None:
                return None
            else:
                variable_names = [self.subwidgets['var_filter_selected'].label]

        if isinstance(variable_names, str):
            variable_names = [ variable_names, ]

        mask = self.variables['name'].isin(variable_names)
        deleted = self.variables[mask]

        # Delete variables
        self.variables = self.variables[~mask]

        # Update selector
        self._update_variables()

        return deleted

    def selected_vars(self):
        """
        Return all the variables in the selected variables box
        """
        return self.subwidgets['var_filter_selected'].options

class DatabaseExplorer(VBox):
    """
    Combo widget based on a select box containing all experiments in
    specified database. 
    """

    session = None
    de = None
    widgets = {}

    def __init__(self, session=None, de=None):

        if de is None: 
            de = DatabaseExplorer(session)
        self.de = de

        self._make_widgets()
        self._set_handlers()

    @staticmethod
    def return_value_or_empty(value):
        """Return value if not None, otherwise empty"""
        if value is None:
            return ''
        else:
            return value

    def _make_widgets(self):

        box_layout = Layout(padding='10px', width='auto', border= '0px solid black')

        style = '<style>p { line-height: 1.4; margin-bottom: 10px }</style>'

        # Gui header
        self.widgets['header'] = HTML(
            value = style + """
            <h3>Database Explorer</h3>

            <p>Select an experiemt to show more detailed information where available.
            With an experiment selected push 'Load Experiment' to open an Experiment 
            Explorer gui.</p>

            <p>The list of experiments can be filtered by keywords and/or variables. 
            Multiple keywords can be selected using alt/option or the shift modifier
            when selecting. To filter by variables select a variable and add it to the
            "Filter variables" box using the ">>" button, and vice-versa to remove
            variables from the filter. Push the 'Filter' button to show only 
            matching experiments.</p>

            <p>The ExperimentExplorer element is accessible as the <tt>ee</tt> attribute
            of the DatabaseExplorer object</p>
            """,
            description='',
            layout={'width': '60%'},
        ) 

        # Experiment selector box
        self.widgets['expt_selector'] = Select(
            options=sorted(self.de.experiments.experiment, key=str.casefold),
            rows=24,
            layout={'padding': '0px 5px', 'width': 'auto'},
            disabled=False
        )

        # Keyword filtering element is a Multiple selection box
        # checkboxes
        self.widgets['filter_widget'] = SelectMultiple(
            rows=15,
            options=sorted(self.de.keywords, key=str.casefold),
            layout={'flex': '0 0 100%'},
        )
        # Reset keywords button
        self.widgets['clear_keywords_button'] = Button(
            description='Clear',
            layout={'width': '20%', 'align': 'center'},
            tooltip='Click to clear selected keywords'
        )
        self.widgets['keyword_box'] = VBox([self.widgets['filter_widget'], 
                                            self.widgets['clear_keywords_button']],
                                            layout={'flex': '0 0 40%'})

        # Filtering button
        self.widgets['filter_button'] = Button(
            description='Filter',
            # layout={'width': '50%', 'align': 'center'},
            tooltip='Click to filter experiments',
        )

        # Variable filter selector combo widget
        self.widgets['var_filter'] = VariableSelectFilter(self.de.variables, layout={'flex': '0 0 40%'})

        # Tab box to contain keyword and variable filters
        self.widgets['filter_tabs'] = Tab(title='Filter', children=[self.widgets['keyword_box'], 
                                                                    self.widgets['var_filter']])
        self.widgets['filter_tabs'].set_title(0, 'Keyword')
        self.widgets['filter_tabs'].set_title(1, 'Variable')

        self.widgets['load_button'] = Button(
            description='Load Experiment',
            disabled=False,
            layout={'width': '50%', },
            tooltip='Click to load experiment'
        )

        # Experiment information panel
        self.widgets['expt_info'] = HTML(
            value='',
            description='',
            layout={'width': '80%', 'align': 'center'},
        )

        # Experiment explorer box
        self.widgets['expt_explorer'] = HBox()

        # Some box layout nonsense to organise widgets in space
        selectors = HBox([
                        VBox([Label(value="Experiments:"), 
                              self.widgets['expt_selector'],
                              self.widgets['load_button'],
                              ],
                              layout={'padding': '0px 5px', 'flex': '0 0 30%'}),
                        VBox([Label(value="Filter by:"), 
                              self.widgets['filter_tabs'],
                              self.widgets['filter_button']],
                              layout={'padding': '0px 10px', 'flex': '0 0 65%'}),
                              #layout=box_layout,),
                        ])

        # Call super init and pass widgets as children
        super().__init__(children=[self.widgets['header'],
                                   selectors,
                                   self.widgets['expt_info'],
                                   self.widgets['expt_explorer']])

    def _set_handlers(self):
        """
        Define routines to handle button clicks and experiment selection
        """
        self.widgets['expt_selector'].observe(self._expt_eventhandler, names='value')
        self.widgets['load_button'].on_click(self._load_experiment)
        self.widgets['filter_button'].on_click(self._filter_experiments)
        self.widgets['clear_keywords_button'].on_click(self._clear_keywords)

    def _filter_restart_eventhandler(selector):
        """
        Re-populate variable list when checkboxes selected/de-selected
        """
        self._filter_variables()

    def _clear_keywords(self, selector):
        """
        Deselect all keywords
        """
        self.widgets['filter_widget'].value = ()

    def _expt_eventhandler(self, selector):
        """
        When experiment is selected populate the experiment information
        elements
        """
        if selector.new is None:
            return
        self._show_experiment_information(selector.new)

    def _show_experiment_information(self, experiment_name):
        """
        Populate box with experiment information
        """
        expt = self.de.experiments[self.de.experiments.experiment == experiment_name]

        style ="""
        <style>
            body  { font: normal 8px Verdana, Arial, sans-serif; }
            table { padding: 10px; border-spacing: 0px 0px; background-color: #fff;" }
            td    { padding: 10px; }
            p     { line-height: 1.2; margin-top: 5px }
        </style>
        """
        self.widgets['expt_info'].value = style + """
        <table>
        <tr><td><b>Experiment:</b></td> <td>{experiment}</td></tr>
        <tr><td style="vertical-align:top;"><b>Description:</b></td> <td><p>{description}</p></td></tr>
        <tr><td><b>Notes:</b></td> <td>{notes}</td></tr>
        <tr><td><b>Contact:</b></td> <td>{contact} &lt;{email}&gt;</td></tr>
        <tr><td><b>No. files:</b></td> <td>{nfiles}</td></tr>
        <tr><td><b>Created:</b></td> <td>{created}</td></tr>
        </table>
        """.format(
                   experiment=experiment_name,
                   description=return_value_or_empty(expt.description.values[0]),
                   notes=return_value_or_empty(expt.notes.values[0]),
                   contact=return_value_or_empty(expt.contact.values[0]),
                   email=return_value_or_empty(expt.email.values[0]),
                   nfiles=return_value_or_empty(expt.ncfiles.values[0]),
                   created=return_value_or_empty(expt.created.values[0]),
                   )
        
    def _filter_experiments(self, b):
        """
        Filter experiment list by keywords and variable
        """
        options = set(self.de.experiments.experiment)

        kwds = self.widgets['filter_widget'].value
        if len(kwds) > 0:
            options.intersection_update(self.de.keyword_filter(kwds))

        variables = self.widgets['var_filter'].selected_vars()
        if len(variables) > 0:
            options.intersection_update(self.de.variable_filter(variables))

        self.widgets['expt_selector'].options = sorted(options, key=str.casefold)

    def _load_experiment(self, b):
        """
        Open an Experiment Explorer UI with selected experiment
        """
        if self.widgets['expt_selector'].value is not None:
            self.ee = ExperimentExplorer(session=self.session, 
                                         experiment=self.widgets['expt_selector'].value)
            self.widgets['expt_explorer'].children = [self.ee]


class ExperimentExplorer(VBox):

    session = None
    data = None
    experiment_name = None
    variables = []
    widgets = {}
    handlers = {}

    def __init__(self, session=None, experiment=None):

        if experiment is None:
            # Have to pass an experiment to DatabaseExtension so that
            # it only creates a variable/keyword map for a single 
            # experiment
            expts = cc.querying.get_experiments(session, all=True)
            experiment = expts.iloc[0].name

        self.de = DatabaseExtension(session, experiments=experiment)

        self.experiment_name = experiment

        self._make_widgets()
        self._load_experiment(self.experiment_name)
        self._set_handlers()

    @staticmethod
    def return_value_or_empty(value):
        """Return value if not None, otherwise empty"""
        if value is None:
            return ''
        else:
            return value

    def _make_widgets(self):

        # Header widget
        self.widgets['header'] = widgets.HTML(
            value="""
            <h3>Experiment Explorer</h3>
            
            <p>Select a variable from the list to display metadata information.
            Where appropriate select a date range. Pressing the <b>Load</b> button
            will read the data into an <tt>xarray DataArray</tt> using the COSIMA Cookook. 
            The command used is output and can be copied and modified as required.</p>

            <p>The loaded DataArray is accessible as the <tt>data</tt> attribute 
            of the ExperimentExplorer object.</p> 
            
            <p>The selected experiment can be changed to any experiment present
            in the current database session.</p>
            """,
            description='',
        )
        
        # Experiment selector element
        self.widgets['expt_selector'] = Dropdown(
            options=sorted(self.de.allexperiments.experiment, key=str.casefold),
            value=self.experiment_name,
            description='',
            layout={'width': '40%'}
        )

        # Date selection widget
        self.widgets['frequency'] = widgets.Dropdown(
            options=(),
            description='Frequency',
            disabled=True,
        )

        # Date selection widget
        self.widgets['daterange'] = widgets.SelectionRangeSlider(
            options=['0000','0001'],
            index=(0,1),
            description='Date range',
            layout={'width': '80%'},
            disabled=True
        )

        # Variable filter selector combo widget. Pass in two widgets so they
        # can be updated by the VariableSelectorInfo widget
        self.widgets['var_selector'] = VariableSelectorInfo(self.de.variables, 
                                                            daterange=self.widgets['daterange'],
                                                            frequency=self.widgets['frequency'],
                                                            rows=20)

        # DataArray information widget
        self.widgets['data_box'] = widgets.HTML()

        # Data load button
        self.widgets['load_button'] = Button(
            description='Load',
            disabled=False,
            layout={'width': '20%', 'align': 'center'},
            tooltip='Click to load data'
        )

        info_pane = VBox([self.widgets['frequency'],
                          self.widgets['daterange']],
                          layout={'padding': '10% 0', 'width': '50%'})

        centre_pane = HBox([VBox([self.widgets['var_selector']]),
                                  info_pane])

        # Call super init and pass widgets as children
        super().__init__(children=[self.widgets['header'],
                                   self.widgets['expt_selector'],
                                   centre_pane,
                                   self.widgets['load_button'],
                                   self.widgets['data_box']])

    def _set_handlers(self):
        """
        Define routines to handle button clicks and experiment selection
        """

        self.widgets['load_button'].on_click(self._load_data)
        self.widgets['expt_selector'].observe(self._expt_eventhandler, names='value')

    def _expt_eventhandler(self, selector):
        """
        Called when experiment dropdown menu changes
        """
        self._load_experiment(selector.new)

    def _load_data(self, b):
        """
        Called when load_button clicked
        """

        data_box = self.widgets['data_box']

        varname = self.widgets['var_selector'].get_selected()
        (start_time, end_time) = self.widgets['daterange'].value
        frequency = self.widgets['frequency'].value

        load_command = """
        <pre><code>cc.querying.getvar('{expt}', '{var}', session, 
                    start_time='{start}', end_time='{end}', frequency='{frequency}')</code></pre>
        """.format(expt=self.widgets['expt_selector'].value, 
                var=varname,
                start=str(start_time),
                end=str(end_time),
                frequency=str(frequency))

        # Interim message to tell user what is happening
        data_box.value = 'Loading data, using following command ...\n\n' + load_command + 'Please wait ... '

        try:
            self.data = cc.querying.getvar(self.experiment_name,
                                    varname,
                                    self.de.session, 
                                    start_time=str(start_time),
                                    end_time=str(end_time),
                                    frequency=frequency)
        except Exception as e:
            data_box.value = data_box.value + 'Error loading variable {} data: {}'.format(varname, e)
            return

        # Update data box with message about command used and pretty HTML
        # representation of DataArray
        data_box.value = 'Loaded data with' + load_command + self.data._repr_html_()

    def _load_experiment(self, experiment_name):
        """
        When first instantiated, or experiment changed, the variable
        selector widget needs to be refreshed
        """
        self.de = DatabaseExtension(self.session, experiments=experiment_name)
        self.experiment_name = experiment_name
        # Add metadata
        self.variables = pd.merge(self.de.variables, 
                                  self.de.get_variables(self.experiment_name), 
                                  how='inner', on=['name', 'long_name'])
        self._load_variables()

    def _load_variables(self):
        """
        Populate the variable selector dialog
        """
        self.widgets['var_selector'].set_variables(self.variables)
        self.widgets['var_selector']._filter_eventhandler(None)

def VariableExplorer(ds):

    ds.hvplot.quadmesh(datashade=True)
