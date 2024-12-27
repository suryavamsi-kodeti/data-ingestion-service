import boto3
import cdsapi
import yaml
import os
import json

def load_config():
    """Loads the YAML configuration file."""
    config_path = os.path.join(os.path.dirname(__file__), "configs", "settings.yaml")
    with open(config_path, "r") as file:
        return yaml.safe_load(file)
    #config_path = os.path.join(os.environ.get("LAMBDA_TASK_ROOT", "/var/task"), "config", "settings.yaml")
    # with open(config_path, "r") as file:
    #     return yaml.safe_load(file)
def get_api_key():
 secrets_manager = boto3.client('secretsmanager')
 secret_arn=os.getenv("CDS_API_KEY")
 response = secrets_manager.get_secret_value(SecretId=secret_arn)
 secret_string = response['SecretString']
 secret = json.loads(secret_string)
 return secret["api_key"]
 
def generate_time_intervals(start_hour, end_hour):
    """Generates a list of hourly time intervals between start_hour and end_hour."""
    return [f"{hour:02d}:00" for hour in range(start_hour, end_hour + 1)]


def lambda_handler(event, context):
    config = load_config()
    
    api_key= get_api_key()
    api_url = config["api"]["url"]
    bucket_name = config["s3"]["bucket_name"]
    base_path = config["s3"]["base_path"]
    variables = config["variables"]
    time_range = config["time_range"]
 
    # Generate time intervals
    time_intervals = generate_time_intervals(
        time_range["start_hour"], time_range["end_hour"]
    )

    # Event details
    year = event["input_time"][0:4]
    month = event["input_time"][5:7]
    day = event["input_time"][8:10]

    print(f"year = {year}, month = {month}, day= {day}")

    filename = f"/tmp/ERA5_hourly_{year}_{str(month).zfill(2)}_{str(day).zfill(2)}.nc"

    # Connect to Copernicus API
    client = cdsapi.Client(url=api_url, key=api_key)
    request = {
        "product_type": "reanalysis",
        "variable": variables,
        "year": f"{year}",
        "month": f"{month}",
        "day": f"{day}",
        "time": time_intervals,
        "format": "netcdf"
    }

    # Fetch and download data
    try:
        client.retrieve("reanalysis-era5-single-levels", request).download(filename)
    except Exception as e:
        print(f"Error while fetching data: {e}")
        return {"status": "error", "message": str(e)}

    # Upload file to S3
    s3 = boto3.client("s3")
    key = f"{base_path}/{year}/{str(month).zfill(2)}/{str(day).zfill(2)}/ERA5_hourly_{year}_{str(month).zfill(2)}_{str(day).zfill(2)}.nc"
    try:
        s3.upload_file(filename, bucket_name, key)
        
    except Exception as e:
        print(f"Error while uploading to S3: {e}")
        return {"status": "error", "message": str(e)}

    # Cleanup temporary file
    if os.path.exists(filename):
        os.remove(filename)
    return {"status": "success", "file_uploaded": key}
    