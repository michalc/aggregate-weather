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

        target = pq.read_table(f.name).to_pandas().to_dict('records')

    with open('fixtures/output.json', 'rb') as f:
        assert json.loads(f.read()) == target
