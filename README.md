# aggregate-weather

Aggregates the hourly weather data from https://api.open-meteo.com/ into days, saving to a parquet file.

By default, aggregates rain and shower values


## Usage

```python
from aggregate import aggregate

aggregate(target_file='weather-aggregate.parquet')
```

## Tests

To run tests:

```shell
pip install -r requirements-dev.txt
pytest
```