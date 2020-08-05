from setuptools import setup, find_packages

setup(
    name='cosima_cookbook',
    description='Diagnostics for COSIMA: Consortium for Ocean-Sea Ice Modelling in Australia',
    url='https://github.com/COSIMA/cosima-cookbook',
    author='COSIMA',
    license='Apache License 2.0',
    use_scm_version=True,
    packages=find_packages(),
    setup_requires=["setuptools_scm"],

    install_requires=[
        'dask',
        'xarray',
        'numpy',
        'matplotlib',
        'bokeh',
        'netcdf4',
        'tqdm',
        'sqlalchemy',
        'cftime',
        'f90nml',
        'joblib',
        'ipywidgets',
    ],

    extras_require = {
        'build': ['distributed', 'pytest']
    }
)
