import json
import tempfile

import pyarrow.parquet as pq

from aggregate import DEFAULT_URL, aggregate


def test_aggregate(httpx_mock):
    with open('fixtures/input.json', 'rb') as f:
        httpx_mock.add_response(
            url=DEFAULT_URL,
            content=f.read(),
        )

    with tempfile.NamedTemporaryFile() as f:
        aggregate(target_file=f.name)

        table = pq.read_table(f.name)
        assert len(table) == 38

