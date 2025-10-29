import datetime

def nanos_to_datetime(nanos):
  return datetime.datetime.fromtimestamp(nanos / (10 ** 9), datetime.timezone.utc)


def nanos_to_timestamp(nanos):
  return nanos_to_datetime(nanos).isoformat()


def nanos_to_year(nanos):
  return nanos_to_datetime(nanos).year
