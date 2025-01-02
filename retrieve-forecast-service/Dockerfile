FROM public.ecr.aws/lambda/python:3.9

WORKDIR /var/task
COPY retrive_forecast.py .
COPY requirements.txt .
RUN pip install -r requirements.txt
CMD ["retrive_forecast.lambda_handler"]