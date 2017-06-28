export MINICONDA=/local/$PROJECT/$USER/miniconda

wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p $MINICONDA
rm miniconda.sh

export PATH="$MINICONDA/bin:$PATH"
hash -r

conda config --set always_yes yes
conda update conda
conda install -n root conda-build
conda install -n root _license

# Useful for debugging any issues with conda
conda info -a

conda create -n cosima_cookbook python=3 jupyter joblib tqdm matplotlib\
                         pandas numpy dask distributed xarray netcdf4\
                         bokeh seaborn datashader python-graphviz
source activate cosima_cookbook

#python setup.py install
python setup.py develop
