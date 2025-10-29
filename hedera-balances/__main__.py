import argparse
import datetime
import decimal
import sys
from decimal import Decimal
from time import sleep
import logging

from google.cloud import bigquery

from .util import nanos_to_timestamp, nanos_to_datetime
from . import native
from . import other

root = logging.getLogger()
root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(threadName)s %(levelname)s %(name)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
  prog='hedera-balances',
  description='Calculate current balance'
)

parser.add_argument("-p", "--project", required=True, help="GCP project to operate in")
parser.add_argument("-d", "--dataset", required=True, help="Dataset containing balances")
parser.add_argument("-x", "--technical-dataset", required=True, help="Dataset containing token transfers")
parser.add_argument("-t", "--token-type", choices=["native", "other"], required=True, help="Token type")
parser.add_argument("-w", "--wait", type=int, default=30, help="How long to wait between indexing rounds")
parser.add_argument("-l", "--latest-table", default="", help="Table reference with latest data")
parser.add_argument("-e", "--end-date", default="", help="When to stop indexing")
parser.add_argument("-m", "--max-batch-duration", type=int, default=5, help="Max batch duration in minutes")

args = parser.parse_args()

project = args.project
dataset = args.dataset
technical_dataset = args.technical_dataset
wait_delay = args.wait
destination = args.latest_table
token_type = args.token_type
end_date = args.end_date
max_batch_duration_in_ns = args.max_batch_duration * 60 * (10 ** 9)

client = bigquery.Client(project=project)

token_aggregator = native if token_type == 'native' else other

global_start = token_aggregator.get_global_start(client, project, dataset, max_batch_duration_in_ns)
logger.info(f"Starting indexing from {global_start}")

while True:
  (start, stop) = token_aggregator.get_latest_unindexed_range(client, project, dataset,
                                                    last_indexed_timestamp=global_start,
                                                    max_batch_duration=max_batch_duration_in_ns,
                                                    end_date=end_date)

  if start is None or stop is None:
    if nanos_to_datetime(global_start).date().isoformat() == end_date:
      token_aggregator.save_latest(client, project, technical_dataset, datetime.date.fromisoformat(end_date), destination)
      break
    else:
      logger.info(f"Waiting for ready blocks greater than {nanos_to_timestamp(global_start)} ({global_start})")

  else:
    logger.info(f"Indexing range from {nanos_to_timestamp(start)} ({start}) to {nanos_to_timestamp(stop)} ({stop})")
    latest_table = destination = token_aggregator.index_range(client, project, technical_dataset, dataset, start, stop, destination)
    global_start = stop
    logger.info(f"Indexed range from {nanos_to_timestamp(start)} ({start}) to {nanos_to_timestamp(stop)} ({stop}) - Results in {latest_table}")

  logger.info(f"Waiting {wait_delay}s for next indexing round")
  sleep(wait_delay)
