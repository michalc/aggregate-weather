import json
import tempfile

from aggregate import aggregate


def test_aggregate():
    with tempfile.NamedTemporaryFile() as f:
        aggregate(target_file=f.name)

        results = json.loads(f.read())
        assert len(results) == 38
