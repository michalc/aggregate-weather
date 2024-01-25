import json

import httpx

DEFAULT_URL='https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31'
DEFAULT_FIELDS=('rain', 'showers')

def aggregate(target_file, source_url=DEFAULT_URL, fields=DEFAULT_FIELDS):
    r = httpx.get(source_url)
    r.raise_for_status()

    source_data = r.json()
    hourly = source_data['hourly']

    results = zip(hourly['time'], hourly['rain'], hourly['showers'])

    with open(target_file, 'wb') as f:
        f.write(json.dumps(list(results)).encode('utf-8'))
