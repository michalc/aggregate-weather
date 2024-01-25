import httpx

DEFAULT_URL='https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31'

def aggregate(target_file, source_url=DEFAULT_URL):
	r = httpx.get(source_url)
	r.raise_for_status()

	with open(target_file, 'wb') as f:
		f.write(r.content)