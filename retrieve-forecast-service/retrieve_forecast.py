import datetime as dt
import os
import re
import shutil
from glob import glob
import numpy as np
import boto3
import xarray as xr
import cfgrib
from ecmwf.opendata import Client
from pathlib import Path


def _fetch_file_list_from_s3(s3_client, bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    filenames = []
    for content in response.get("Contents", []):
        filenames.append(content["Key"].split("/")[-1])
    return filenames


def _download_file_from_s3(bucket_name, prefix, download_folder, file_pattern):
    s3 = boto3.client("s3")
    continuation_token = None
    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix,ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        include_file = re.compile(file_pattern)
        if "Contents" in response:
            for obj in response["Contents"]:
                file_name =  obj["Key"].split("/")[-1]
                if include_file.match(file_name):
                    local_path = os.path.join(download_folder, file_name)
                    print(f"Downloading {file_name}")
                    s3.download_file(bucket_name, obj["Key"], local_path)
        else:
            print("No files found for the specified path.")
        continuation_token = response.get('NextContinuationToken')
        if not continuation_token:
            break


def _remove_folder(folder_path):
    try:
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            print(f"Folder '{folder_path}' and all its contents have been deleted.")
        else:
            print(f"Folder '{folder_path}' does not exist.")
    except Exception as e:
        print(f"Error: {e}")


def _remove_file(file_path):
    try:
        os.remove(file_path)
    except FileNotFoundError:
        print(f"{file_path} does not exist.")
    except PermissionError:
        print(f"Permission denied to delete {file_path}.")
    except Exception as e:
        print(f"An error occurred: {e}")


def _delete_files(pattern):
    files_to_delete = Path('.').glob(pattern)
    for file in files_to_delete:
        try:
            file.unlink()  # Deletes the file
        except Exception as e:
            print(f"Error deleting {file}: {e}")


def retrieve_GFS_forecast(year, month, day, hour):
    client = boto3.client("s3")
    filenames = _fetch_file_list_from_s3(client, "non-cat-weather-data", "forecasts/")

    filename = "GFS_t2m_fcst_{}{}{}{}.nc".format(
        year, str(month).zfill(2), str(day).zfill(2), str(hour).zfill(2)
    )
    if filename in filenames:
        print(
            "GFS forecast {}-{}-{}-{} already present on TMHCC S3.".format(
                year, month, day, hour
            )
        )
        return

    download_folder_path = "./gdownload"
    src_folder_path = f"gfs.{year}{str(month).zfill(2)}{str(day).zfill(2)}/{str(hour).zfill(2)}/atmos/"
    formatted_hour = str(hour).zfill(2)
    files_to_download = r"gfs\.t" + formatted_hour + r"z\.pgrb2\.1p00\.f.{3}$"
    _download_file_from_s3(
        "noaa-gfs-bdp-pds", src_folder_path, download_folder_path,
        files_to_download
    )
    temps = []
    times = []
    file_list = glob(f"{download_folder_path}/gfs*pgrb2*")
    if len(file_list) != 129:  # No files present
        print("Forecast not found at provider or forecast not complete.")
        if len(file_list) != 0:
            _remove_folder(download_folder_path)
        return
    file_list.sort()
    ds = xr.open_dataset(
        file_list[0],
        engine="cfgrib",
        filter_by_keys={"typeOfLevel": "heightAboveGround", "shortName": "2t"},
    )
    years = ds.time.dt.year.values
    months = ds.time.dt.month.values
    days = ds.time.dt.day.values
    hours = ds.time.dt.hour.values

    for file in file_list:
        try:
            ds = xr.open_dataset(
                file,
                engine="cfgrib",
                filter_by_keys={"typeOfLevel": "heightAboveGround", "shortName": "2t"},
            )
        except:
            pass
        step = int(file.split("p00.f")[1])
        basehour = int(file.split("fs.t")[1].split("z.p")[0])
        lats = ds.latitude.values
        lons = ds.longitude.values
        temps.append(ds["t2m"].values)
        times.append(step)

    temps = np.array(temps)

    # Write to netCDF
    ds_out = xr.Dataset(
        {"t2m": (["time", "latitude", "longitude"], temps)},
        coords={"time": times, "latitude": lats, "longitude": lons},
    )

    ds_out.to_netcdf(filename)
    _remove_folder(download_folder_path)
    s3 = boto3.resource("s3")
    s3.Bucket("non-cat-weather-data").upload_file(
        filename, "forecasts/{}".format(filename)
    )
    _remove_file(filename)
    print(
        "GFS forecast {}-{}-{}-{} downloaded and moved to TMHCC S3.".format(
            year, month, day, hour
        )
    )
    return


def retrieve_AIFS_forecast(year, month, day, hour):
    # Check whether file is already present on S3 bucket
    client = boto3.client("s3")

    filenames = _fetch_file_list_from_s3(client, "non-cat-weather-data",
                                         "forecasts/")

    filename = "AIFS_t2m_fcst_{}{}{}{}.nc".format(
        year, str(month).zfill(2), str(day).zfill(2), str(hour).zfill(2)
    )
    if filename in filenames:
        print(
            "AIFS forecast {}-{}-{}-{} already present on TMHCC S3.".format(
                year, month, day, hour
            )
        )
        return

    download_folder_path = "./adownlaod"
    src_folder_path = f"{year}{month}{day}/{hour}z/aifs/0p25/oper/"
    files_to_download = r".*grib2"
    _download_file_from_s3(
        "ecmwf-forecasts", src_folder_path, download_folder_path,
        files_to_download
    )

    fns = glob(f"{download_folder_path}/*grib2")
    if len(fns) == 0:
        print("No forecast found at provider.")
        return
    ds = xr.open_mfdataset(
        f"{download_folder_path}/*grib2",
        engine="cfgrib",
        filter_by_keys={"typeOfLevel": "heightAboveGround", "level": 2},
        combine="nested",
        concat_dim="step",
    )
    encoding = {"t2m": {"zlib": True, "complevel": 3}}
    ds["t2m"].sortby("step").to_netcdf(filename, encoding=encoding)
    _remove_folder(download_folder_path)
    s3 = boto3.resource("s3")
    s3.Bucket("non-cat-weather-data").upload_file(
        filename, "forecasts/{}".format(filename)
    )
    _remove_file(filename)
    print(
        "AIFS forecast {}-{}-{}-{} downloaded and moved to TMHCC S3.".format(
            year, month, day, hour
        )
    )
    return


def retrieve_IFS_forecast(year, month, day, hour):
    # Check whether file is already present on S3 bucket
    client = boto3.client("s3")
    filenames = _fetch_file_list_from_s3(client, "non-cat-weather-data",
                                         "forecasts/")
    filename = "IFS_t2m_fcst_{}{}{}{}.nc".format(
        year, str(month).zfill(2), str(day).zfill(2), str(hour).zfill(2)
    )
    if filename in filenames:
        print(
            "IFS forecast {}-{}-{}-{} already present on TMHCC S3.".format(
                year, month, day, hour
            )
        )
        return
    download_folder_path = "./idownlaod"
    src_folder_path = f"{year}{month}{day}/{hour}z/aifs/0p25/oper/"
    files_to_download = r".*grib2"
    _download_file_from_s3(
        "ecmwf-forecasts", src_folder_path, download_folder_path,
        files_to_download
    )
    fns = glob(f"{download_folder_path}/*grib2")
    if len(fns) == 0:  # No grib files present
        print("No forecast found at provider.")
        return
    ds = xr.open_mfdataset(
        "*grib2",
        engine="cfgrib",
        filter_by_keys={"typeOfLevel": "heightAboveGround", "level": 2},
        combine="nested",
        concat_dim="step",
    )
    encoding = {"t2m": {"zlib": True, "complevel": 3}}
    ds["t2m"].sortby("step").to_netcdf(filename, encoding=encoding)
    _remove_folder(download_folder_path)
    s3 = boto3.resource("s3")
    s3.Bucket("non-cat-weather-data").upload_file(
        filename, "forecasts/{}".format(filename)
    )
    _remove_file(filename)
    print(
        "IFS forecast {}-{}-{}-{} downloaded and moved to TMHCC S3.".format(
            year, month, day, hour
        )
    )
    return


def retrieve_IFS_ensemble_forecast(year, month, day, hour):
    # Check whether file is already present on S3 bucket
    client = boto3.client("s3")
    filenames = _fetch_file_list_from_s3(client, "non-cat-weather-data",
                                         "forecasts/")
    filename = "IFS_enfo_t2m_fcst_cf_{}{}{}{}.nc".format(
        year, str(month).zfill(2), str(day).zfill(2), str(hour).zfill(2)
    )
    if filename in filenames:
        print(
            "IFS ensemble forecast {}-{}-{}-{} already present on TMHCC S3."
            .format(
                year, month, day, hour
            )
        )
        return
    client = Client("ecmwf", beta=True)
    parameters = ["2t"]
    filename = "IFS_enso.grib"
    steps = list(range(0, 360, 6))
    try:
        client.retrieve(
            date="{}{}{}".format(year, month, day),
            time="{}".format(hour),
            step=steps,
            stream="enfo",
            type=["cf", "pf"],
            levtype="sfc",
            param=parameters,
            target=filename,
        )
    except:
        print("No forecast present for IFS enfo")
        return
    ds = xr.open_dataset(filename, filter_by_keys={"dataType": "pf"})
    for i in range(50):
        ds.sel(number=i + 1).resample(valid_time="1D").mean().to_netcdf(
            "IFS_enfo_t2m_fcst_pf{}_{}{}{}{}.nc".format(
                str(i + 1).zfill(2),
                year,
                str(month).zfill(2),
                str(day).zfill(2),
                str(hour).zfill(2),
            )
        )
    ds.close()

    ds = xr.open_dataset(filename, filter_by_keys={"dataType": "cf"})
    ds.resample(valid_time="1D").mean().to_netcdf(
        "IFS_enfo_t2m_fcst_cf_{}{}{}{}.nc".format(
            year, str(month).zfill(2), str(day).zfill(2), str(hour).zfill(2)
        )
    )
    ds.close()
    _remove_file(filename)
    s3 = boto3.resource("s3")

    for i in range(50):
        filename = "IFS_enfo_t2m_fcst_pf{}_{}{}{}{}.nc".format(
            str(i + 1).zfill(2),
            year,
            str(month).zfill(2),
            str(day).zfill(2),
            str(hour).zfill(2),
        )
        s3.Bucket("non-cat-weather-data").upload_file(
            filename, "forecasts/{}".format(filename)
        )
        _remove_file(filename)

    filename = "IFS_enfo_t2m_fcst_cf_{}{}{}{}.nc".format(
        year, str(month).zfill(2), str(day).zfill(2), str(hour).zfill(2)
    )
    s3.Bucket("non-cat-weather-data").upload_file(
        filename, "forecasts/{}".format(filename)
    )
    _remove_file(filename)
    print(
        "IFS ensemble forecast {}-{}-{}-{} downloaded and moved to TMHCC S3."
        .format(
            year, month, day, hour
        )
    )
    return


def lambda_handler(event, context):
    today = dt.date.today()
    start_day = today - dt.timedelta(days=15)
    run_days = [today - dt.timedelta(days=i) for i in range(15)]
    for day in run_days:
        for hour in [0,6,12,18]:
            #retrieve_GFS_forecast(day.year, day.month, day.day,
            #                      str(hour).zfill(2))
            
            retrieve_AIFS_forecast(
                day.year,
                str(day.month).zfill(2),
                str(day.day).zfill(2),
                str(hour).zfill(2),
            )
            break
        # for hour in [0, 12]:
        #     retrieve_IFS_ensemble_forecast(
        #         day.year,
        #         str(day.month).zfill(2),
        #         str(day.day).zfill(2),
        #         str(hour).zfill(2),
        #     )
        
        
if __name__ == "__main__":
    lambda_handler({},{})