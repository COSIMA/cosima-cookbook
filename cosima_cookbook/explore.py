from logging import warning
import lxml.html
import re
import warnings

from ipywidgets import HTML, Button, VBox, HBox, Label, Layout, Select
from ipywidgets import SelectMultiple, Tab, Text, Textarea, Checkbox
from ipywidgets import SelectionRangeSlider, Dropdown
import pandas as pd
from sqlalchemy import func
import xarray as xr

from . import database, querying
from .database import CFVariable, NCFile, NCExperiment, NCVar
from .date_utils import format_datetime, parse_datetime


def return_value_or_empty(value):
    """Return value if not None, otherwise empty"""
    if value is None:
        return ""
    else:
        # Strip out html tags for safety
        return lxml.html.fromstring(str(value)).text_content()


class DatabaseExtension:

    # DEPRECATED

    def __init__(self, session, experiments=None):
        """ """
        warnings.warn("DatabaseExtension is deprecated and no longer required")


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

    def __init__(self, variables, rows=10, **kwargs):
        """
        variables is a pandas dataframe. kwargs are passed through to child
        widgets which, theoretically, allows for layout information to be
        specified
        """
        self._make_widgets(rows)
        super().__init__(
            children=[
                self.model,
                self.search,
                self.selector,
                self.info,
                self.filter_coords,
                self.filter_restarts,
            ],
            **kwargs
        )
        self.set_variables(variables)
        self._set_info()
        self._set_observes()

    def _make_widgets(self, rows):
        """
        Instantiate all widgets
        """

        # Experiment selector element
        self.model = Dropdown(
            options=(),
            layout={"padding": "0px 5px", "width": "initial"},
            description="",
        )
        # Variable search
        self.search = Text(
            placeholder="Search: start typing",
            layout={"padding": "0px 5px", "width": "auto", "overflow-x": "scroll"},
        )
        # Variable selection box
        self.selector = Select(
            options=(),  # sorted(self.variables.name, key=str.casefold),
            rows=rows,
            layout=self.search.layout,
        )
        # Variable info
        self.info = HTML(layout=self.search.layout)
        # Variable filtering elements
        self.filter_coords = Checkbox(
            value=True,
            indent=False,
            description="Hide coordinates",
        )
        self.filter_restarts = Checkbox(
            value=True,
            indent=False,
            description="Hide restarts",
        )

    def _set_observes(self):
        """
        Set event handlers
        """
        self.filter_coords.observe(self._filter_eventhandler, names="value")
        self.filter_restarts.observe(self._filter_eventhandler, names="value")

        self.model.observe(self._model_eventhandler, names="value")
        self.search.observe(self._search_eventhandler, names="value")
        self.selector.observe(self._selector_eventhandler, names="value")

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
        options = {"All models": ""}
        for model in variables.model.cat.categories.values:
            if len(model) > 0 and model != "none":
                options["{} only".format(model.capitalize())] = model
        self.model.options = options

        options = dict()
        firstvar = None
        for vals in variables.sort_values(["name"])[
            ["name", "long_name", "units"]
        ].values:

            var, name, units = map(str, vals)

            if firstvar is None:
                firstvar = var

            if name.lower() == "none" or name == "":
                name = var

            # Add units string if suitable value exists
            if (
                units.lower() == "none"
                or units.lower() == "nounits"
                or units.lower() == "no units"
                or units.lower() == "dimensionless"
                or units.lower() == "1"
                or units == ""
            ):
                options[var] = "{}".format(name)
            else:
                options[var] = "{} ({})".format(name, units)

        # Populate variable selector
        self.selector.options = options

        # Highlight first value, otherwise accessors like .value are not
        # immediately accessible
        if firstvar is not None:
            self.selector.value = options[firstvar]

    def _reset_filters(self):
        """
        Reset filters to default values
        """
        self.filter_coords.value = True
        self.filter_restarts.value = True

    def _model_eventhandler(self, event=None):
        """
        Filter by model
        """
        model = self.model.value

        # Reset the coord and restart filters when a model changed
        self._reset_filters()
        self._filter_variables(model=model)

    def _filter_eventhandler(self, event=None):
        """
        Called when filter button pushed
        """
        self._filter_variables(
            self.filter_coords.value, self.filter_restarts.value, self.model.value
        )

    def _filter_variables(self, coords=True, restarts=True, model=""):
        """
        Optionally hide some variables
        """
        # Set up a mask with all true values
        mask = self.variables["name"] != ""

        # Filter for matching models
        if model != "":
            mask = mask & (self.variables["model"] == model)

        # Conditionally filter out restarts and coordinates
        if coords:
            mask = mask & ~self.variables["coordinate"]
        if restarts:
            mask = mask & ~self.variables["restart"]

        # Mask out hidden variables
        self.variables["visible"] = mask

        # Update the variable selector
        self._update_selector(self.variables[self.variables.visible])

        # Reset the search
        self.search.value = ""
        self.selector.value = None

    def _search_eventhandler(self, event=None):
        """
        Live search bar, updates the selector options dynamically, does not alter
        visible mask in variables
        """
        search_term = self.search.value

        variables = self.variables[self.variables.visible]
        if search_term is not None or search_term != "":
            try:
                variables = variables[
                    variables.name.str.contains(search_term, case=False, na=False)
                    | variables.long_name.str.contains(
                        search_term, case=False, na=False
                    )
                ]
            except:
                warnings.warn("Illegal character in search!", UserWarning)
                search_term = self.search.value

        self._update_selector(variables)

    def _selector_eventhandler(self, event=None):
        """
        Update variable info when variable selected
        """
        self._set_info(self.selector.value)

    def _set_info(self, long_name=None):
        """
        Set long name info widget
        """
        if long_name is None or long_name == "":
            long_name = "&nbsp;"
        style = "<style>.breakword { word-wrap: break-word; font-size: 90%; line-height: 1.1;}</style>"
        self.info.value = style + '<p class="breakword">{long_name}</p>'.format(
            long_name=long_name
        )

    def delete(self, variable_names=None):
        """
        Remove variables
        """
        # If no variable specified just delete the currently selected one
        if variable_names is None:
            if self.selector.label is None:
                return None
            else:
                variable_names = [
                    self.selector.label,
                ]

        if isinstance(variable_names, str):
            variable_names = [
                variable_names,
            ]

        mask = self.variables["name"].isin(variable_names)
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
        return self.selector.label


class VariableSelectorInfo(VariableSelector):
    """
    Subclass of VariableSelector to display more info in a separate widget
    """

    def __init__(
        self, parent, variables, daterange, frequency, cellmethods, rows=10, **kwargs
    ):

        # The cellmethods widget needs access to the session and experiment
        self.session = parent.session
        self.experiment = parent.experiment_name

        super(VariableSelectorInfo, self).__init__(variables, rows, **kwargs)

        # Requires three widgets passed as arguments. An html box where
        # extended meta-data will be displayed, a date range widget for
        # selecting start and end times to load, and a cellmethods dropdown
        # selection box
        self.daterange = daterange
        self.frequency = frequency
        self.cellmethods = cellmethods

        # Set event handlers. Don't use _set_observes method: don't want
        # it called when super init invoked
        self.selector.observe(self._var_eventhandler, names="value")
        self.frequency.observe(self._frequency_eventhandler, names="value")
        self.cellmethods.observe(self._cellmethods_eventhandler, names="value")

        # Set default filtering
        # self._filter_variables()

    def _var_eventhandler(self, selector):
        """
        Called when variable selected
        """
        self._set_frequency_selector(self.selector.label)

    def _frequency_eventhandler(self, selector):
        """
        When frequency selector is changed update cellmethods selector
        and daterange slider
        """
        self._set_cellmethods_selector(self.selector.label, self.frequency.value)

    def _cellmethods_eventhandler(self, selector):
        """
        When cellmethods selector is changed update daterange slider
        """
        self._set_daterange_selector(
            self.selector.label, self.frequency.value, self.cellmethods.value
        )

    def _set_frequency_selector(self, variable_name):
        """
        Populate the variable loading selectors widgets for daterange,
        frequency and cellmethods given a variable name
        """
        variable = self.variables.loc[self.variables["name"] == variable_name]

        # Initialise daterange widget
        self.daterange.options = ["0000", "0000"]
        self.daterange.disabled = True

        self.frequency.options = []
        self.frequency.disabled = True

        if len(variable) > 0:
            self.frequency.options = set(variable.frequency)
            self.frequency.index = 0
            self.frequency.disabled = False

    def _set_cellmethods_selector(self, variable_name, frequency):
        """
        When frequency selector is changed update cellmethods dropdown
        """
        # Find the matching variable in our list
        self.cellmethods.options = []
        self.cellmethods.disabled = True

        # Note frequency comparison done against underlying numpy array
        # in case frequency is None, which is a legitimate value, but
        # comparing to None doesn't work for pandas
        self.cellmethods.options = set(
            self.variables[
                (self.variables["name"] == variable_name)
                & (self.variables["frequency"].values == frequency)
            ].cell_methods
        )

        if len(self.cellmethods.options) > 0:
            self.cellmethods.index = 0
            self.cellmethods.disabled = False

    def _set_daterange_selector(self, variable_name, frequency, cellmethods):
        """
        When frequency selector is changed update daterange slider
        """
        # Find the matching variable in our list
        variable = self.variables.loc[
            (self.variables["name"] == variable_name)
            & (self.variables["frequency"] == frequency)
            & (self.variables["cell_methods"] == cellmethods)
        ]
        try:
            # Populate daterange widget if variable contains necessary information
            # Convert human readable frequency to pandas compatigle frequency string
            freq = re.sub(
                r"^(\d+) (\w)(\w+)", r"\1\2", str(variable.frequency.values[0]).upper()
            )
            dates = xr.cftime_range(
                parse_datetime(variable.time_start.values[0]),
                parse_datetime(variable.time_end.values[0]),
                freq=freq,
            )
            self.daterange.options = [(i.strftime("%Y/%m/%d"), i) for i in dates]
            self.daterange.value = (dates[0], dates[-1])
        except:
            pass
        finally:
            self.daterange.disabled = False


class VariableSelectFilter(HBox):
    """
    Combo widget which contains a VariableSelector from which variables can
    be transferred to another Select Widget to specify which variables should
    be used to filter experiments
    """

    variables = pd.DataFrame()

    def __init__(self, selvariables, **kwargs):
        """
        selvariables is a dataframe and is used to populate the VariableSelector

        self.variables contains the variables transferred to the selected widget
        """
        self._make_widgets(selvariables, **kwargs)

        super().__init__(
            children=[self.selector, self.button_box, self.filter_box], **kwargs
        )

        self._set_observes()

    def _make_widgets(self, selvariables, **kwargs):
        """
        Instantiate all widgets
        """
        layout = {"padding": "0px 5px"}
        # Variable selector combo-widget. Pass only unique variable names. Variable
        # name is the only value used for filtering, so will pick up all matches
        self.selector = VariableSelector(selvariables.drop_duplicates("name"), **kwargs)

        # Button to add variable from selector to selected
        self.var_filter_add = Button(
            tooltip="Add selected variable to filter",
            icon="angle-double-right",
            layout={"width": "auto"},
        )
        # Button to add variable from selector to selected
        self.var_filter_sub = Button(
            tooltip="Remove selected variable from filter",
            icon="angle-double-left",
            layout={"width": "auto"},
        )
        self.button_box = VBox(
            [self.var_filter_add, self.var_filter_sub],
            layout={"padding": "100px 5px", "height": "100%"},
        )
        # Selected variables for filtering with header widget
        self.var_filter_label = HTML("Filter variables:", layout=layout)
        self.var_filter_selected = Select(
            options=[],
            rows=10,
            layout=layout,
        )
        self.filter_box = VBox(
            [self.var_filter_label, self.var_filter_selected], layout=layout
        )

    def _set_observes(self):
        """
        Set event handlers
        """
        self.var_filter_add.on_click(self._add_var_to_selected)
        self.var_filter_sub.on_click(self._sub_var_from_selected)

    def _update_variables(self):
        """
        Update filtered variables
        """
        self.var_filter_selected.options = dict(
            self.variables.sort_values(["name"])[["name", "long_name"]].values
        )

    def _add_var_to_selected(self, button):
        """
        Transfer variable from selector to filtered variables
        """
        self.add(self.selector.delete())

    def add(self, variable=None):
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
        self.selector.add(self.delete())

    def delete(self, variable_names=None):
        """
        Delete variable from list of filtered variables
        """
        # If no variable specified just delete the currently selected one
        if variable_names is None:
            if self.var_filter_selected.label is None:
                return None
            else:
                variable_names = [self.var_filter_selected.label]

        if isinstance(variable_names, str):
            variable_names = [
                variable_names,
            ]

        mask = self.variables["name"].isin(variable_names)
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
        return self.var_filter_selected.options


class DatabaseExplorer(VBox):
    """
    Combo widget based on a select box containing all experiments in
    specified database.
    """

    session = None
    ee = None
    experiments = None
    keywords = None
    variables = None

    def __init__(self, session=None, de=None):

        if session is None:
            session = database.create_session()
        self.session = session

        if de is not None:
            warning.warn("DatabaseExtension has been deprecated is no longer supported")

        self.experiments = querying.get_experiments(session=self.session, all=True)
        self.keywords = sorted(querying.get_keywords(self.session), key=str.casefold)
        self.variables = querying.get_variables(self.session, inferred=True)

        self._make_widgets()

        # Call super init and pass widgets as children
        super().__init__(
            children=[self.header, self.selectors, self.expt_info, self.expt_explorer]
        )

        # Show the experiment information: important for only one experiment, as
        # events will not trigger this otherwise
        self._show_experiment_information(self.expt_selector.value)
        self._set_handlers()

    def _make_widgets(self):

        style = "<style>.header p{ line-height: 1.4; margin-bottom: 10px }</style>"

        # Gui header
        self.header = HTML(
            value=style
            + """
            <h3>Database Explorer</h3>

            <div class="header">

            <p>Select an experiment to show more detailed information where available.
            With an experiment selected push 'Load Experiment' to open an Experiment
            Explorer gui.</p>

            <p>The list of experiments can be filtered by keywords and/or variables.
            Multiple keywords can be selected using alt/option/ctrl (system dependent)
            or the shift modifier when selecting. To filter by variables select a
            variable and add it to the "Filter variables" box using the ">>" button,
            and vice-versa to remove variables from the filter. Push the 'Filter'
            button to show only matching experiments.</p>

            <p>When the ExperimentExplorer element loads data it is accessible as the
            <tt>.data</tt> attribute of the DatabaseExplorer object</p>

            </div>
            """,
            description="",
            layout={"width": "60%"},
        )

        # Experiment selector box
        self.expt_selector = Select(
            options=sorted(set(self.experiments.experiment), key=str.casefold),
            rows=24,
            layout={"padding": "0px 5px", "width": "auto"},
            disabled=False,
        )

        # Keyword filtering element is a Multiple selection box
        # checkboxes
        self.filter_widget = SelectMultiple(
            rows=15,
            options=sorted(self.keywords, key=str.casefold),
            layout={"flex": "0 0 100%"},
        )
        # Reset keywords button
        self.clear_keywords_button = Button(
            description="Clear",
            layout={"width": "20%", "align": "center"},
            tooltip="Click to clear selected keywords",
        )
        self.keyword_box = VBox(
            [self.filter_widget, self.clear_keywords_button], layout={"flex": "0 0 40%"}
        )

        # Filtering button
        self.filter_button = Button(
            description="Filter",
            # layout={'width': '50%', 'align': 'center'},
            tooltip="Click to filter experiments",
        )

        # Variable filter selector combo widget
        self.var_filter = VariableSelectFilter(
            self.variables, layout={"flex": "0 0 40%"}
        )

        # Tab box to contain keyword and variable filters
        self.filter_tabs = Tab(
            title="Filter", children=[self.keyword_box, self.var_filter]
        )
        self.filter_tabs.set_title(0, "Keyword")
        self.filter_tabs.set_title(1, "Variable")

        self.load_button = Button(
            description="Load Experiment",
            disabled=False,
            layout={
                "width": "50%",
            },
            tooltip="Click to load experiment",
        )

        # Experiment information panel
        self.expt_info = HTML(
            value="",
            description="",
            layout={"width": "80%", "align": "center"},
        )

        # Experiment explorer box
        self.expt_explorer = HBox()

        # Some box layout nonsense to organise widgets in space
        self.selectors = HBox(
            [
                VBox(
                    [
                        Label(value="Experiments:"),
                        self.expt_selector,
                        self.load_button,
                    ],
                    layout={"padding": "0px 5px", "flex": "0 0 30%"},
                ),
                VBox(
                    [Label(value="Filter by:"), self.filter_tabs, self.filter_button],
                    layout={"padding": "0px 10px", "flex": "0 0 65%"},
                ),
            ]
        )

    def _keyword_filter(self, keywords):
        """
        Return a list of experiments matching *all* of the supplied keywords
        """
        try:
            return querying.get_experiments(self.session, keywords=keywords).experiment
        except AttributeError:
            return []

    def _variable_filter(self, variables):
        """
        Return a set of experiments that contain all the defined variables
        """
        return querying.get_experiments(self.session, variables=variables).experiment

    def _set_handlers(self):
        """
        Define routines to handle button clicks and experiment selection
        """
        self.expt_selector.observe(self._expt_eventhandler, names="value")
        self.load_button.on_click(self._load_experiment)
        self.filter_button.on_click(self._filter_experiments)
        self.clear_keywords_button.on_click(self._clear_keywords)

    def _filter_restart_eventhandler(self, selector):
        """
        Re-populate variable list when checkboxes selected/de-selected
        """
        self._filter_variables()

    def _clear_keywords(self, selector):
        """
        Deselect all keywords
        """
        self.filter_widget.value = ()

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
        expt = self.experiments[self.experiments.experiment == experiment_name]

        style = """
        <style>
            .info { font: normal 90% Verdana, Arial, sans-serif; }
            .info a:hover { color: red; text-decoration: underline; }
        </style>
        """
        self.expt_info.value = (
            style
            + """
        <div class="info">
        <table>
        <tr><td><b>Experiment:</b></td> <td>{experiment}</td></tr>
        <tr><td style="vertical-align:top;"><b>Description:</b></td> <td>{description}</td></tr>
        <tr><td style="vertical-align:top;"><b>Notes:</b></td> <td>{notes}</td></tr>
        <tr><td><b>Contact:</b></td> <td>{contact} &lt;<a href="mailto:{email}" target="_blank">{email}</a>&gt;</td></tr>
        <tr><td><b>Control repo:</b></td> <td><a href="{url}" target="_blank">{url}</a></td></tr>
        <tr><td><b>No. files:</b></td> <td>{ncfiles}</td></tr>
        <tr><td><b>Created:</b></td> <td>{created}</td></tr>
        </table>
        </div>
        """.format(
                experiment=experiment_name,
                **{
                    field: return_value_or_empty(expt[field].values[0])
                    for field in [
                        "description",
                        "notes",
                        "contact",
                        "email",
                        "url",
                        "ncfiles",
                        "created",
                    ]
                }
            )
        )

    def _filter_experiments(self, b):
        """
        Filter experiment list by keywords and variable
        """
        options = set(self.experiments.experiment)

        kwds = self.filter_widget.value
        if len(kwds) > 0:
            options.intersection_update(self._keyword_filter(kwds))

        variables = self.var_filter.selected_vars()
        if len(variables) > 0:
            options.intersection_update(self._variable_filter(variables))

        self.expt_selector.options = sorted(options, key=str.casefold)

    def _load_experiment(self, b):
        """
        Open an Experiment Explorer UI with selected experiment
        """
        if self.expt_selector.value is not None:
            self.ee = ExperimentExplorer(
                session=self.session, experiment=self.expt_selector.value
            )
            self.expt_explorer.children = [self.ee]

    @property
    def data(self):
        """
        Return xarray DataArray if one has been loaded in ExperimentExplorer
        """
        if self.ee is None:
            print("Cannot return data if no experiment has been loaded")
            return None

        return self.ee.data


class ExperimentExplorer(VBox):

    session = None
    _loaded_data = None
    experiment_name = None
    variables = None
    experiments = None

    def __init__(self, session=None, experiment=None):

        if session is None:
            session = database.create_session()
        self.session = session

        self.experiments = querying.get_experiments(session=self.session, all=True)

        if self.experiments.size == 0:
            raise ValueError("No experiments found in database")

        if experiment is None:
            experiment = self.experiments.iloc[0].experiment

        self.experiment_name = experiment
        self.variables = querying.get_variables(
            self.session, self.experiment_name, inferred=True
        )

        self._make_widgets()

        # Call super init and pass widgets as children
        super().__init__(
            children=[
                self.header,
                self.expt_selector,
                self.centre_pane,
                self.load_button,
                self.data_box,
            ]
        )

        # self._load_experiment(self.experiment_name)
        self._load_variables()
        self._set_handlers()

    def _make_widgets(self):
        # Header widget
        self.header = HTML(
            value="""
            <h3>Experiment Explorer</h3>

            <p>Select a variable from the list to display metadata information.
            Where appropriate select a date range. Pressing the <b>Load</b> button
            will read the data into an <tt>xarray DataArray</tt> using the COSIMA Cookook. 
            The command used is output and can be copied and modified as required.</p>

            <p>The loaded DataArray is accessible as the <tt>.data</tt> attribute 
            of the ExperimentExplorer object.</p> 

            <p>The selected experiment can be changed to any experiment present
            in the current database session.</p>
            """,
            description="",
        )
        # Experiment selector element
        self.expt_selector = Dropdown(
            options=sorted(set(self.experiments.experiment), key=str.casefold),
            value=self.experiment_name,
            description="",
            layout={"width": "40%"},
        )
        # Frequency selection widget
        self.frequency = Dropdown(
            options=(),
            description="Frequency",
            disabled=True,
        )
        # Cell methods selection widget
        self.cellmethods = Dropdown(
            options=(),
            style={"description_width": "initial"},
            description="Cell methods",
            disabled=True,
        )
        # Date selection widget
        self.daterange = SelectionRangeSlider(
            options=["0000", "0001"],
            index=(0, 1),
            description="Date range",
            layout={"width": "40%"},
            disabled=True,
        )
        # Variable filter selector combo widget. Pass in two widgets so they
        # can be updated by the VariableSelectorInfo widget
        self.var_selector = VariableSelectorInfo(
            self,
            self.variables,
            daterange=self.daterange,
            frequency=self.frequency,
            cellmethods=self.cellmethods,
            rows=20,
        )
        # DataArray information widget
        self.data_box = HTML()
        # Data load button
        self.load_button = Button(
            description="Load",
            disabled=False,
            layout={"width": "20%", "align": "center"},
            tooltip="Click to load data",
        )
        self.info_pane = VBox(
            [self.frequency, self.cellmethods, self.daterange],
            layout={"padding": "10% 0", "width": "80%"},
        )
        self.centre_pane = HBox([VBox([self.var_selector]), self.info_pane])

    def _set_handlers(self):
        """
        Define routines to handle button clicks and experiment selection
        """
        self.load_button.on_click(self._load_data)
        self.expt_selector.observe(self._expt_eventhandler, names="value")

    def _expt_eventhandler(self, selector):
        """
        Called when experiment dropdown menu changes
        """
        self._load_experiment(selector.new)

    def _load_data(self, b):
        """
        Called when load_button clicked
        """
        varname = self.var_selector.get_selected()
        (start_time, end_time) = self.daterange.value
        frequency = self.frequency.value
        cellmethods = self.cellmethods.value

        # Create a dict to build load command and the
        # string representation of the same load command
        kwargs = {
            "session": self.session,
            "expt": self.expt_selector.value,
            "variable": varname,
            "frequency": frequency,
            "attrs": {"cell_methods": cellmethods},
            "start_time": str(start_time),
            "end_time": str(end_time),
            "n": 1,
        }

        load_command = """cc.querying.getvar(expt='{expt}', variable='{variable}', 
                          session=session, frequency='{frequency}'"""
        if cellmethods is not None:
            load_command = (
                load_command
                + """,
                          attrs={attrs}"""
            )
        if frequency == "static":
            load_command = load_command + ", n={n})"
        else:
            load_command = (
                load_command
                + """,
                          start_time='{start_time}', 
                          end_time='{end_time}')"""
            )

        # Format load_command string
        load_command = load_command.format(**kwargs)
        load_command = "<pre><code>" + load_command + "</code></pre>"

        # Interim message to tell user what is happening
        self.data_box.value = (
            "Loading data, using following command ..."
            + load_command
            + "Please wait ... "
        )

        if frequency == "static":
            del kwargs["start_time"]
            del kwargs["end_time"]
        else:
            del kwargs["n"]

        if cellmethods is None:
            del kwargs["attrs"]

        try:
            self._loaded_data = querying.getvar(**kwargs)
        except Exception as e:
            self.data_box.value = (
                self.data_box.value
                + "Error loading variable {} data: {}".format(varname, e)
            )
            return

        # Update data box with message about command used and pretty HTML
        # representation of DataArray
        self.data_box.value = (
            "Loaded data with"
            + load_command
            + self._loaded_data._repr_html_()
            + "Data can be accessed through .data attribute"
        )

    def _load_experiment(self, experiment_name):
        """
        When first instantiated, or experiment changed, the variable
        selector widget needs to be refreshed
        """
        self.experiment_name = experiment_name
        self.variables = querying.get_variables(
            self.session, self.experiment_name, inferred=True
        )
        self._load_variables()

    def _load_variables(self):
        """
        Populate the variable selector dialog
        """
        self.var_selector.set_variables(self.variables)
        self.var_selector._filter_eventhandler(None)

    @property
    def data(self):
        """
        Return xarray DataArray if one has been loaded
        """
        if self._loaded_data is None:
            print("No data can be returned: no variable has been loaded")

        return self._loaded_data
