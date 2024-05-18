# Weather Pipeline

This project provides a weather data pipeline written in Python 3.

## Installation

To install the required dependencies, run:

```bash
pip install -r requirements.txt
```

## Usage
To run the weather pipeline, execute the following command:

```bash
python3 weather-pipeline.py
```

## Nocodb install

```bash
docker run -d --name nocodb-postgres \
-v "$(pwd)"/nocodb:/usr/app/data/ \
-p 8085:8085 \
-e NC_DB="pg://34.143.211.214:5432?u=root&p=123456a&d=nocodb" \
-e NC_AUTH_JWT_SECRET="569a1821-0a93-45e8-87ab-eb857f20a010" \
-e PORT=8085 \
-e NUXT_PUBLIC_NC_BACKEND_URL="http://127.0.0.1:8085" nocodb/nocodb:latest
```

```bash
ssh -L 8085:127.0.0.1:8085 112.137.129.246 -p 2050
```