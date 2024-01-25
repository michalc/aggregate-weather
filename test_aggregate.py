import json

from aggregate import aggregate


def test_aggregate():
    target_file = 'aggregated.json'
    aggregate(target_file=target_file)

    with open(target_file, 'rb') as f:
        results = json.loads(f.read())
    assert 'latitude' in results
