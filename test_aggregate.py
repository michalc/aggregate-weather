from aggregate import aggregate


def test_aggregate():
	result = aggregate()
	assert 'latitude' in result
