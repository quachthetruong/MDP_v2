# 
FROM ghcr.io/dask/dask:2023.5.0 
#version need match on client 
RUN apt-get update \
    && apt-get -y install  curl

USER root
# 
WORKDIR /app

# 
COPY ./requirements.txt requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r requirements.txt
