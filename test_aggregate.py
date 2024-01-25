import json
import tempfile

import pyarrow.parquet as pq

from aggregate import aggregate



def test_aggregate():
    with tempfile.NamedTemporaryFile() as f:
        aggregate(target_file=f.name)

        table = pq.read_table(f.name)
        assert len(table) == 38
