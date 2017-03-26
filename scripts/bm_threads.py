import netCDF4

filename = '/g/data1/v45/mom01_comparison/KDS75/output165/ocean.nc'

def get_slice(i):
    dataset = netCDF4.Dataset(filename)
    temp = dataset.variables['temp']
    res = temp[0, i, :, :].mean()
    dataset.close()
    return res

import concurrent.futures

# We can use a with statement to ensure threads are cleaned up promptly
with concurrent.futures.ProcessPoolExecutor() as executor:
    # Start the load operations and mark each future with its URL
    future_to_i = {executor.submit(get_slice, i): i for i in range(75)}
    for future in concurrent.futures.as_completed(future_to_i):
        i = future_to_i[future]
        try:
            data = future.result()
        except Exception as exc:
            print('%r generated an exception: %s' % (i, exc))

