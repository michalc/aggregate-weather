import itertools
import json
from datetime import date
from functools import reduce

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

    summed_by_day = {
        day: reduce(lambda total, item: total + item[1:], items, (0,) * len(fields))
        for day, items in grouped_by_day
    }
    summed_by_day_dicts = [
        {
            'day': day,
            **{
                field: values[i]
                for i, field in enumerate(fields)
            }
        }
        for day, values in summed_by_day.items()
    ]

    with open(target_file, 'wb') as f:
        f.write(json.dumps(summed_by_day_dicts).encode('utf-8'))
