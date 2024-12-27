import datetime as dt
from glob import glob
import numpy as np
import datetime as dt
import s3fs
import subprocess
import boto3
import xarray as xr
import cfgrib
from ecmwf.opendata import Client 

try:
    from netCDF4 import Dataset
except:
    !pip install xarray[complete]
    !pip install netCDF4
    from netCDF4 import Dataset



def retrieve_GFS_forecast(year, month, day, hour):
    # Check whether file is already present on S3 bucket
    client = boto3.client('s3')

    response = client.list_objects_v2(
        Bucket='non-cat-weather-data',
        Prefix='forecasts/')
    filenames = []
    for content in response.get('Contents', []):
        filenames.append(content['Key'].split('/')[-1])


    filename = 'GFS_t2m_fcst_{}{}{}{}.nc'.format(year, str(month).zfill(2), str(day).zfill(2),str(hour).zfill(2))
    if filename in filenames:
        print('GFS forecast {}-{}-{}-{} already present on TMHCC S3.'.format(year, month, day, hour))
        return

    !aws s3 cp --no-sign-request s3://noaa-gfs-bdp-pds/gfs.{year}{str(month).zfill(2)}{str(day).zfill(2)}/{str(hour).zfill(2)}/atmos/ . --recursive --exclude "*" --include "gfs.t{str(hour).zfill(2)}z.pgrb2.1p00.f???" --quiet
    temps = []
    times = []

    file_list = glob('gfs*pgrb2*')
    if len(file_list) != 129: # No files present
        print('Forecast not found at provider or forecast not complete.')
        if len(file_list) != 0:
            !rm gfs*pgrb2*
        return
    file_list.sort()
    ds = xr.open_dataset(file_list[0], engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'shortName':'2t'})
    years = ds.time.dt.year.values
    months = ds.time.dt.month.values
    days = ds.time.dt.day.values
    hours = ds.time.dt.hour.values

    for file in file_list:
        try:
            ds = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'shortName':'2t'})
        except:
            pass
        step = int(file.split('p00.f')[1])
        basehour = int(file.split('fs.t')[1].split('z.p')[0])
        lats = ds.latitude.values
        lons = ds.longitude.values
        temps.append(ds['t2m'].values)
        times.append(step)

    temps = np.array(temps)

    # Write to netCDF
    ds_out = xr.Dataset(
        {
            't2m': (['time','latitude', 'longitude'], temps)
        },
        coords = {
            'time': times,
            'latitude': lats,
            'longitude': lons
        }
    )

    ds_out.to_netcdf(filename)
    !rm gfs*pgrb2*
    s3 = boto3.resource('s3')
    s3.Bucket('non-cat-weather-data').upload_file(filename, 'forecasts/{}'.format(filename))
    subprocess.run(["rm", "/home/ec2-user/SageMaker/{}".format(filename)])
    print('GFS forecast {}-{}-{}-{} downloaded and moved to TMHCC S3.'.format(year, month, day, hour))
    return
===============================================================
def retrieve_AIFS_forecast(year, month, day, hour):
    # Check whether file is already present on S3 bucket
    client = boto3.client('s3')

    response = client.list_objects_v2(
        Bucket='non-cat-weather-data',
        Prefix='forecasts/')
    filenames = []
    for content in response.get('Contents', []):
        filenames.append(content['Key'].split('/')[-1])
    
    filename = 'AIFS_t2m_fcst_{}{}{}{}.nc'.format(year, str(month).zfill(2), str(day).zfill(2),str(hour).zfill(2))
    if filename in filenames:
        print('AIFS forecast {}-{}-{}-{} already present on TMHCC S3.'.format(year, month, day, hour))
        return
    !aws s3 cp --no-sign-request s3://ecmwf-forecasts/{year}{month}{day}/{hour}z/aifs/0p25/oper/ . --recursive --exclude "*" --include "*grib2" --quiet
    fns = glob('*grib2')
    if len(fns) == 0: # No grib files present
        print('No forecast found at provider.')
        return
    ds = xr.open_mfdataset('*grib2',engine='cfgrib',filter_by_keys={'typeOfLevel':'heightAboveGround','level':2}, combine='nested',concat_dim='step')
    encoding = {"t2m": {'zlib': True, "complevel":3}}
    ds['t2m'].sortby('step').to_netcdf(filename, encoding=encoding)
    !rm *grib2
    !rm *idx
    s3 = boto3.resource('s3')
    s3.Bucket('non-cat-weather-data').upload_file(filename, 'forecasts/{}'.format(filename))
    subprocess.run(["rm", "/home/ec2-user/SageMaker/{}".format(filename)])
    print('AIFS forecast {}-{}-{}-{} downloaded and moved to TMHCC S3.'.format(year, month, day, hour))
    return
================================================================================
def retrieve_IFS_forecast(year, month, day, hour):
    # Check whether file is already present on S3 bucket
    client = boto3.client('s3')

    response = client.list_objects_v2(
        Bucket='non-cat-weather-data',
        Prefix='forecasts/')
    filenames = []
    for content in response.get('Contents', []):
        filenames.append(content['Key'].split('/')[-1])
    filename = 'IFS_t2m_fcst_{}{}{}{}.nc'.format(year, str(month).zfill(2), str(day).zfill(2),str(hour).zfill(2))
    if filename in filenames:
        print('IFS forecast {}-{}-{}-{} already present on TMHCC S3.'.format(year, month, day, hour))
        return
    !aws s3 cp --no-sign-request s3://ecmwf-forecasts/{year}{month}{day}/{hour}z/ifs/0p25/oper/ . --recursive --exclude "*" --include "*grib2" --quiet
    fns = glob('*grib2')
    if len(fns) == 0: # No grib files present
        print('No forecast found at provider.')
        return
    ds = xr.open_mfdataset('*grib2', engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level' :2}, combine='nested', concat_dim='step')
    encoding = {"t2m": {'zlib': True, "complevel":3}}
    ds['t2m'].sortby('step').to_netcdf(filename, encoding=encoding)
    !rm *grib2
    !rm *idx
    s3 = boto3.resource('s3')
    s3.Bucket('non-cat-weather-data').upload_file(filename, 'forecasts/{}'.format(filename))
    subprocess.run(["rm", "/home/ec2-user/SageMaker/{}".format(filename)])
    print('IFS forecast {}-{}-{}-{} downloaded and moved to TMHCC S3.'.format(year, month, day, hour))
    return
====================================================================
def retrieve_IFS_ensemble_forecast(year, month, day, hour):
    # Check whether file is already present on S3 bucket
    client = boto3.client('s3')

    response = client.list_objects_v2(
        Bucket='non-cat-weather-data',
        Prefix='forecasts/')
    filenames = []
    for content in response.get('Contents', []):
        filenames.append(content['Key'].split('/')[-1])
    filename = 'IFS_enfo_t2m_fcst_cf_{}{}{}{}.nc'.format(year, str(month).zfill(2), str(day).zfill(2),str(hour).zfill(2))
    if filename in filenames:
        print('IFS ensemble forecast {}-{}-{}-{} already present on TMHCC S3.'.format(year, month, day, hour))
        return
    client = Client("ecmwf", beta=True)
    parameters = ['2t']
    filename = 'IFS_enso.grib'
    steps=list(range(0,360,6))
    try:
        client.retrieve(
            date='{}{}{}'.format(year, month, day),
            time='{}'.format(hour),
            step=steps,
            stream="enfo",
            type=['cf', 'pf'],
            levtype="sfc",
            param=parameters,
            target=filename
        )
    except:
        print('No forecast present for IFS enfo')
        return
    
    ds = xr.open_dataset(filename, filter_by_keys={'dataType': 'pf'})
    for i in range(50):
        ds.sel(number=i+1).resample(valid_time='1D').mean().to_netcdf('IFS_enfo_t2m_fcst_pf{}_{}{}{}{}.nc'.format(str(i+1).zfill(2), year, str(month).zfill(2), str(day).zfill(2),str(hour).zfill(2)))
    ds.close()

    ds = xr.open_dataset(filename, filter_by_keys={'dataType': 'cf'})
    ds.resample(valid_time='1D').mean().to_netcdf('IFS_enfo_t2m_fcst_cf_{}{}{}{}.nc'.format(year, str(month).zfill(2), str(day).zfill(2),str(hour).zfill(2)))
    ds.close()
    subprocess.run(["rm", "/home/ec2-user/SageMaker/{}".format(filename)])
    s3 = boto3.resource('s3')

    for i in range(50):
        filename = 'IFS_enfo_t2m_fcst_pf{}_{}{}{}{}.nc'.format(str(i+1).zfill(2), year, str(month).zfill(2), str(day).zfill(2),str(hour).zfill(2))
        s3.Bucket('non-cat-weather-data').upload_file(filename, 'forecasts/{}'.format(filename))
        subprocess.run(["rm", "/home/ec2-user/SageMaker/{}".format(filename)])

    filename = 'IFS_enfo_t2m_fcst_cf_{}{}{}{}.nc'.format(year, str(month).zfill(2), str(day).zfill(2),str(hour).zfill(2))
    s3.Bucket('non-cat-weather-data').upload_file(filename, 'forecasts/{}'.format(filename))
    subprocess.run(["rm", "/home/ec2-user/SageMaker/{}".format(filename)])
    !rm *idx
    print('IFS ensemble forecast {}-{}-{}-{} downloaded and moved to TMHCC S3.'.format(year, month, day, hour))
    return
====================================================================================================
today = dt.date.today()
start_day = today-dt.timedelta(days=15)
run_days = [today-dt.timedelta(days=i) for i in range(15)]

for day in run_days: #[yesterday, today]:
    for hour in [0, 6, 12, 18]:
        retrieve_GFS_forecast(day.year, day.month, day.day, str(hour).zfill(2))
        retrieve_AIFS_forecast(day.year, str(day.month).zfill(2), str(day.day).zfill(2), str(hour).zfill(2))
    for hour in [0, 12]:
        #retrieve_IFS_forecast(day.year, str(day.month).zfill(2), str(day.day).zfill(2), str(hour).zfill(2))
        retrieve_IFS_ensemble_forecast(day.year, str(day.month).zfill(2), str(day.day).zfill(2), str(hour).zfill(2))