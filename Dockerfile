#main image
FROM python:3.8.8-slim AS main
RUN apt-get update && apt-get -y install gcc
RUN pip install cython
RUN pip install pandas
RUN pip install cymem
RUN pip install pyyaml
RUN pip install boto3
RUN python -m pip install --upgrade pip
RUN pip install -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.4.1/tested_requirements/requirements_36.reqs
RUN pip install snowflake-connector-python==2.4.1
RUN pip install pyarrow==3.0.0
RUN mkdir -p /app
COPY main.py /app/
COPY worker.py /app/
COPY heap.pyx /app/
COPY heap.pxd /app/
COPY typedef.pxd /app/
COPY tdsp.pyx /app/
COPY read_ctl_s3.py /app/
COPY read_network_snowflake.py /app/
COPY read_trips_snowflake.py /app/
COPY write_output_snowflake.py /app/
COPY control.yaml /app/
RUN mkdir /output
VOLUME /output
EXPOSE 8000
CMD ["python", "/app/main.py"]