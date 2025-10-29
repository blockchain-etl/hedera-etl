import datetime
import decimal
import logging
from decimal import Decimal

from .util import nanos_to_timestamp

logger = logging.getLogger(__name__)

def get_global_start(client, project, dataset, max_batch_size):
  logger.info(f"Looking for latest ingested balance")
  # Perform a query.
  latest_balance_query = f"""
    SELECT MAX(created) AS latest FROM `{project}.{dataset}.token_balance` WHERE created >= "2019-01-01"
    """

  query_job = client.query(latest_balance_query)  # API request
  rows = query_job.result()  # Waits for query to finish

  row = next(rows)
  latest = row.latest
  if latest is None:
    earliest_block_query = f"""
    SELECT MIN(timestamp.from) AS earliest FROM `{project}.{dataset}.block` WHERE created >= "2019-01-01"
    """

    query_job = client.query(earliest_block_query)  # API request
    rows = query_job.result()  # Waits for query to finish

    row = next(rows)
    latest = row.earliest

    return latest - (max_batch_size / 2)

  else:
    if isinstance(latest, datetime.datetime):
      return int((Decimal(latest.timestamp()) * 10 ** 9)
                 .to_integral_exact(decimal.ROUND_FLOOR))
    else:
      return int((Decimal(datetime.datetime.fromisoformat(latest).timestamp()) * 10 ** 9)
                 .to_integral_exact(decimal.ROUND_FLOOR))


def get_latest_unindexed_range(client, project, dataset, last_indexed_timestamp, max_batch_duration):
  logger.info(f"Querying for ready blocks after {last_indexed_timestamp}")

  # Perform a query.
  query = f"""
  WITH
  block AS (
    SELECT *
    FROM `{project}.{dataset}.block`
    WHERE created > "{nanos_to_timestamp(last_indexed_timestamp)}"
  ),
  transactions AS (
    SELECT COUNT(*) AS transaction_count, b.count, b.name, b.timestamp.to AS consensus_timestamp,
    FROM `{project}.{dataset}.transaction` t
    JOIN block b ON consensus_timestamp BETWEEN b.timestamp.from AND b.timestamp.to
    WHERE t.created > "{nanos_to_timestamp(last_indexed_timestamp)}" AND t.created <= "{nanos_to_timestamp(last_indexed_timestamp + max_batch_duration)}"
    GROUP by b.name, b.count, b.name, b.timestamp.to
  ),
  block_with_transactions AS (
    SELECT b.* FROM block b JOIN transactions t ON b.timestamp.to = t.consensus_timestamp AND b.`count` = t.`count`
  ),
  latest_block AS (
    SELECT AS VALUE MIN(b1.name) FROM block_with_transactions b1 FULL OUTER JOIN block_with_transactions b2 ON b1.hash = b2.previous_hash
    WHERE b2.hash IS NULL
  ),
  ready_blocks AS (
    SELECT AS STRUCT name, timestamp.from, timestamp.to
    FROM block b, latest_block lb
    WHERE b.name <= lb AND b.timestamp.from > {last_indexed_timestamp}
  ),
  upper_bound AS (
    SELECT AS VALUE MIN(`from`) + {max_batch_duration} FROM ready_blocks
  )
  SELECT MIN(`from`) AS `min`, MAX(`to`) AS `max` FROM ready_blocks, upper_bound WHERE `to` < upper_bound
  """

  query_job = client.query(query)  # API request
  rows = query_job.result()  # Waits for query to finish

  row = next(rows)
  return (row.min, row.max)


def index_range(client, project, technical_dataset, dataset, start, stop, latest_table):
  source = f"""
  (
    SELECT created, account_id, token_id, serial_number, consensus_timestamp, amount, block_timestamp_to, transaction_id
    FROM `{project}.{technical_dataset}.token_transfer`
    WHERE consensus_timestamp BETWEEN {start} AND {stop} AND created >= "{nanos_to_timestamp(start)}" AND created <= "{nanos_to_timestamp(stop)}"
    UNION ALL
    SELECT created,  account_id, token_id, serial_number, consensus_timestamp, amount, block_timestamp_to, transaction_id
    FROM (
      SELECT created, account_id, token_id, serial_number, consensus_timestamp, amount, block_timestamp_to, transaction_id,
      ROW_NUMBER() OVER (PARTITION BY account_id, token_id, serial_number ORDER BY consensus_timestamp DESC) AS rn
      FROM `{latest_table}`
    )
    WHERE rn = 1
  )""" if latest_table else f"""
  `{project}.{technical_dataset}.token_transfer` WHERE consensus_timestamp BETWEEN {start} AND {stop} AND created >= "{nanos_to_timestamp(start)}" AND created <= "{nanos_to_timestamp(stop)}"
  """

  # Perform a query.
  query = f"""
  SELECT
    created, account_id, token_id, serial_number, consensus_timestamp, block_timestamp_to, transaction_id, SUM(amount)
    OVER
    (
      PARTITION BY account_id
      ORDER BY consensus_timestamp ASC
    ) AS amount
  FROM {source}
  """

  query_job = client.query(query)  # API request
  rows = query_job.result()  # Waits for query to finish

  latest_dest = query_job.destination
  latest_table = f"{latest_dest.project}.{latest_dest.dataset_id}.{latest_dest.table_id}"

  insert_query = f"""
  INSERT `{project}.{dataset}.token_balance` (created, account_id, token_id, serial_number, consensus_timestamp, amount, block_timestamp_to, transaction_id)
  SELECT created, account_id, token_id, serial_number, consensus_timestamp, amount,  block_timestamp_to, transaction_id FROM `{latest_table}`
  WHERE consensus_timestamp >= {start}
  """

  query_job = client.query(insert_query)  # API request
  rows = query_job.result()  # Waits for query to finish

  return latest_table
