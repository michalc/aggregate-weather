import itertools
import json
from datetime import datetime, date
from functools import reduce

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_URL='https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31'
DEFAULT_FIELDS=('rain', 'showers')

def aggregate(target_file, source_url=DEFAULT_URL, fields=DEFAULT_FIELDS):
    # If the source data isn't in the expected form, in most cases there will be an exception
    # that should help debug. Save for example if some of the  "fields" are not present in the
    # source data there will be a key error

    # Fetch data, raising an exception on non-200
    r = httpx.get(source_url)
    r.raise_for_status()

    # Group data by day
    source_data = r.json()
    hourly = source_data['hourly']
    results = zip(hourly['time'], *(hourly[field] for field in fields))
    grouped_by_day = itertools.groupby(sorted(results), key=lambda item: datetime.strptime(item[0][:10], "%Y-%m-%d").date())

    # Sum data by day
    summed_by_day = {
        day: reduce(
            (lambda total, item: total + item[1:]),
            items,
            (0,) * len(fields),
        )
        for day, items in grouped_by_day
    }

    # Convert to list of dicts
    # Tthis makes it fairly straightforward to debug and also save via pandas
    summed_by_day_dicts = [
        {
            'day': str(day),
            **{
                field: values[i]
                for i, field in enumerate(fields)
            }
        }
        for day, values in summed_by_day.items()
    ]

    # Save as parquet file
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(summed_by_day_dicts)), target_file)
