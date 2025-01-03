from SpeedwellClimateAPI import SpeedwellAPISession
from SpeedwellClimateAPI import SpeedwellDataAPI
import pandas as pd
import json
import datetime
import boto3
from io import StringIO
import os

BUCKET_NAME = "non-cat-weather-data"

def get_speedwellsession(username, password, operaton_name):
    session = SpeedwellAPISession()
    session.ConnectToDataAPI(operaton_name, username, password)
    return session


def get_station_history(session, wStationCode, WIDElement,
                        startdate=None, enddate=None,):
    try:
        wQueryStartDate = "{}-{}-{}".format(
            startdate.year, str(startdate.month).zfill(2),
            str(startdate.day).zfill(2)
        )
    except:
        wQueryStartDate = "1900-01-01"
    try:
        wQueryEndDate = "{}-{}-{}".format(
            enddate.year, str(enddate.month).zfill(2),
            str(enddate.day).zfill(2)
        )
    except:
        enddate = datetime.date.today()
        wQueryEndDate = "{}-{}-{}".format(
            enddate.year, str(enddate.month).zfill(2),
            str(enddate.day).zfill(2)
        )
    
    # The three settings below define which quality of data you want and in which order of priority
    wUseBestDataTypeHierarchy = True  # When true, the API will return data using the data type hierarchy as defined by default by Speedwell Climate
    wIDDataTypeHierarchy = None
    wIDDataTypes = (
        []
    )  # If you prefer to use your own specific hierarchy instead of the Speedwell's default one, please enter the ID of the Data Types you would like to use here

    if (wUseBestDataTypeHierarchy == False) and (
        len(wIDDataTypes) == 0
    ):  # user forgot to pass in some Data Types, rectifying using Cleaned Data Types.
        wIDDataTypes.append(SpeedwellDataAPI.eDataType.CLIMATE_CLEANED.value)
        wIDDataTypes.append(SpeedwellDataAPI.eDataType.CLIMATE_SYNTHETIC.value)
        wIDDataTypes.append(SpeedwellDataAPI.eDataType.SYNOP_CLEANED.value)

    wDesiredMinutes = None

    oResults = session.DataAPI.GetHistoricalData(
        wIDDataTypes,
        WIDElement,
        wStationCode,
        wQueryStartDate,
        wQueryEndDate,
        wIDDataTypeHierarchy,
        wUseBestDataTypeHierarchy,
        wDesiredMinutes,
    )
   
    if oResults is not None:
        return oResults.to_dataframe()
    return None


def write_to_s3(dataframe, station, variable):
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer)
    s3_resource = boto3.resource("s3")
    s3_resource.Object(
        BUCKET_NAME,
        "speedwell_station/{}/{}/speedwell_{}_{}.csv".format(
            station, variable, station, variable
        ),
    ).put(Body=csv_buffer.getvalue())


def process_station_history(session, station, variable):
    if variable == "tmin":
        varcode = 12
    if variable == "tmax":
        varcode = 11
    if variable == "tmean":
        varcode = 10
    if variable == "thour":
        varcode = 29
    s3 = boto3.resource("s3")
    try:
        s3.Object(
            BUCKET_NAME,
            "speedwell_station/{}/{}/speedwell_{}_{}.csv".format(
                station, variable, station, variable
            ),
        ).load()
        client = boto3.client("s3")
        object_key = "speedwell_station/{}/{}/speedwell_{}_{}.csv".format(
            station, variable, station, variable
        )
        csv_obj = client.get_object(Bucket=BUCKET_NAME, Key=object_key)
        body = csv_obj["Body"]
        csv_string = body.read().decode("utf-8")
        hist_df = pd.read_csv(StringIO(csv_string))
        hist_df["Dates"] = pd.to_datetime(hist_df["Dates"])
        d = hist_df["Dates"].max()

        lastdate = datetime.date(d.year, d.month, d.day) - datetime.timedelta(
            days=5)
        

        new_df = get_station_history(session, station, varcode,
                                     startdate=lastdate)
        
      
        df = (
            pd.concat([hist_df, new_df])
            .groupby("Dates")
            .first()[["Values", "DataTypes"]]
        )
        print(lastdate, len(hist_df), len(new_df), len(df))
    except Exception as e:
        print(e)
        print("Retrieve history.")

    write_to_s3(df, station, variable)
    return


def fetch_api_creds(secret_arn):
 secrets_manager = boto3.client('secretsmanager')

 response = secrets_manager.get_secret_value(SecretId=secret_arn)
 secret_string = response['SecretString']
 api_config = json.loads(secret_string)
 return api_config

def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    object_key = "speedwell_station/exposure_stations.txt"
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
    body = response["Body"]
    csv_string = body.read().decode("utf-8")
    df = pd.read_csv(StringIO(csv_string))
    api_secret_arn = os.getenv("SPEEDWELL_API_DETAILS_ARN")
    api_configuration = fetch_api_creds(api_secret_arn)

    speedwell_session = get_speedwellsession(
        api_configuration["user_name"],
        api_configuration["password"],
        api_configuration["api_config"]
    )

    for i in range(len(df)):
        print(f'Start processing {df["SRC_ID"]}')
        process_station_history(speedwell_session, df["SRC_ID"][i], "tmin")
        process_station_history(speedwell_session, df["SRC_ID"][i], "tmax")
        process_station_history(speedwell_session, df["SRC_ID"][i], "tmean")
        process_station_history(speedwell_session, df["SRC_ID"][i], "thour")
        print(f'Processed {i}, {len(df)}, {df["SRC_ID"][i]}')
    return {"status": "success"}
