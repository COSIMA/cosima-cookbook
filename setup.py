from setuptools import setup, find_packages

setup(
    name='cosima_cookbook',
    version='0.2',
    description='Diagnostics for COSIMA: Consortium for Ocean-Sea Ice Modelling in Australia',
    url='https://github.com/COSIMA/cosima-cookbook',
    author='James Munroe',
    author_email='jmunroe@mun.ca',
    license='Apache License 2.0',
    packages=find_packages(),

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
    ],

    extras_require = {
        'build': ['distributed', 'pytest']
    }
)
