import itertools
import json
from datetime import date

import httpx

DEFAULT_URL='https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31'
DEFAULT_FIELDS=('rain', 'showers')

def aggregate(target_file, source_url=DEFAULT_URL, fields=DEFAULT_FIELDS):
    r = httpx.get(source_url)
    r.raise_for_status()

    source_data = r.json()
    hourly = source_data['hourly']

    results = zip(hourly['time'], *(hourly[field] for field in fields))
    grouped_by_day = itertools.groupby(sorted(results), key=lambda item: item[0][:10])
    grouped_by_day_materialized = {key: list(items) for key, items in grouped_by_day}

    with open(target_file, 'wb') as f:
        f.write(json.dumps(grouped_by_day_materialized).encode('utf-8'))
