import click
import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_exponential, 
    retry_if_exception_type,
    RetryCallState,
    before_sleep_log
)
import traceback
import psycopg2
from psycopg2.extras import Json
import json
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from ..models.chunk import DayChunk, ChunkPeriod
from ..storage.postgres_manager import PostgresManager
import logging
import sys

# Set up logging
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger(__name__)

def log_retry_attempt(retry_state: RetryCallState):
    """Custom callback for logging retry attempts"""
    # Get chunk info from the args (database, query, chunk)
    chunk = retry_state.args[2] if len(retry_state.args) > 2 else None
    
    if chunk:
        try:
            # Try to handle both datetime and string formats
            start_time = chunk[0].isoformat() if hasattr(chunk[0], 'isoformat') else chunk[0]
            end_time = chunk[1].isoformat() if hasattr(chunk[1], 'isoformat') else chunk[1]
            chunk_info = f"chunk {start_time} to {end_time}"
        except (AttributeError, IndexError):
            # Fallback if chunk format is unexpected
            chunk_info = f"chunk {chunk}"
    else:
        chunk_info = "unknown chunk"

    if retry_state.attempt_number == 1:
        logger.info(f"First attempt for {chunk_info}")
    else:
        error_info = ""
        if retry_state.outcome and retry_state.outcome.failed:
            error = retry_state.outcome.exception()
            error_info = f" (Error: {str(error)})"

        logger.warning(
            "Retrying %s: attempt %d ended with: %s%s for %s",
            retry_state.fn.__name__,
            retry_state.attempt_number,
            retry_state.outcome,
            error_info,
            chunk_info
        )
    return True

class TrafficCollector:
    def __init__(self, cluster_url="https://akshuba.centralus.kusto.windows.net",
                 days_to_collect=2, chunk_minutes=15, max_workers=16):
        self.cluster_url = cluster_url
        self.DAYS_TO_COLLECT = days_to_collect
        self.CHUNK_MINUTES = chunk_minutes
        self.MAX_WORKERS = max_workers
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_url)
        self.client = KustoClient(kcsb)
        self.pg_manager = PostgresManager()

    def get_day_chunks(self) -> List[DayChunk]:
        """Generate time chunks aligned to full days."""
        end_time = (datetime.utcnow() - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999)
        start_time = end_time - timedelta(days=self.DAYS_TO_COLLECT - 1)

        # Align to full days (midnight to midnight UTC)
        start_date = start_time.date()
        end_date = end_time.date()

        day_chunks = []
        current_date = start_date

        while current_date <= end_date:
            # Always use full day boundaries (00:00:00 to 23:59:59)
            day_start = datetime.combine(current_date, time.min)
            day_end = datetime.combine(current_date, time.max).replace(microsecond=999999)

            day_chunks.append(DayChunk(
                date=current_date,
                start_time=day_start,
                end_time=day_end
            ))
            current_date += timedelta(days=1)

        return day_chunks

    def get_time_chunks(self, day_chunk: DayChunk) -> List[tuple]:
        """Generate time chunks for a specific day based on minutes."""
        chunks = []
        current = day_chunk.start_time

        # Calculate total seconds and chunk seconds
        total_seconds = (day_chunk.end_time - day_chunk.start_time).total_seconds()
        chunk_seconds = self.CHUNK_MINUTES * 60
        num_chunks = int((total_seconds + chunk_seconds - 1) // chunk_seconds)

        for _ in range(num_chunks):
            chunk_end = min(current + timedelta(minutes=self.CHUNK_MINUTES), day_chunk.end_time)
            chunks.append((current, chunk_end))
            current = chunk_end
            if current >= day_chunk.end_time:
                break

        # Validate chunk coverage
        if chunks:
            assert chunks[0][0] == day_chunk.start_time, "First chunk should start at day start"
            assert chunks[-1][1] == day_chunk.end_time, "Last chunk should end at day end"
            for i in range(len(chunks)-1):
                assert chunks[i][1] == chunks[i+1][0], "Chunks should be continuous"

        # Debug info
        click.echo(f"\nChunks for {day_chunk.date}:")
        click.echo(f"Start time: {day_chunk.start_time}")
        click.echo(f"End time: {day_chunk.end_time}")
        click.echo(f"Number of chunks: {len(chunks)}")

        return chunks

    def build_query(self, component: str, start_time: datetime, end_time: datetime) -> str:
        """
        Build Kusto query for a specific time chunk with enhanced pod lifetime tracking.
        
        Args:
            component: The target component (e.g., 'msi-connector')
            start_time: Query start time
            end_time: Query end time
            
        Returns:
            str: Formatted Kusto query
        """
        query = f"""
        let endTime = datetime('{end_time.isoformat()}');
        let startTime = datetime('{start_time.isoformat()}');
        let targetComponent = '{component}';
        let underlayNamePrefix = "hcp-underlay-eastus-cx";

        // Enhanced pod lifetimes with all needed information
        let podLifetimes =
            cluster('{self.cluster_url}').database('AKSinfra').ProcessInfo
            | where TIMESTAMP between (startTime-15m .. endTime)
            | where UnderlayName startswith underlayNamePrefix
            | where State == "running"
            | where isnotempty(PodIP)
            | summarize 
                PodStartTime = min(TIMESTAMP),
                PodEndTime = max(TIMESTAMP),
                PodLabels = take_any(PodLabels),
                PodNamespace = take_any(PodNamespace)
                by PodName, PodIP, UnderlayName;

        // Get msi-connector requests with cluster context
        let relevantIPsWithCluster = 
            cluster('{self.cluster_url}').database('AKSprod').IncomingRequestTrace
            | where UnderlayName startswith underlayNamePrefix
            | where TIMESTAMP between (startTime .. endTime)
            | where namespace == targetComponent
            | where isnotempty(clientRemoteAddr)
            | extend clientIP = tostring(split(clientRemoteAddr, ':')[0])
            | where isnotempty(clientIP)
            | project 
                clientIP,
                UnderlayName,
                RequestTime = TIMESTAMP;

        // Process pod information with cluster awareness
        podLifetimes
        | join kind=inner hint.strategy=shuffle (
            relevantIPsWithCluster
        ) on $left.PodIP == $right.clientIP and $left.UnderlayName == $right.UnderlayName
        | where RequestTime between (PodStartTime .. PodEndTime)
        | extend isCCP = tostring(PodNamespace matches regex "^[0-9]")
        | extend parsedPodLabels = todynamic(PodLabels)
        | extend LabelKey = case(
            isCCP == "True" and isnotnull(parsedPodLabels['kube-egress-gateway-control-plane']), 'kube-egress-gateway-control-plane',
            isCCP == "True" and isnotnull(parsedPodLabels['rsName']), 'rsName',
            isCCP == "True" and isnotnull(parsedPodLabels['control-plane']), 'control-plane',
            isCCP == "True" and isnotnull(parsedPodLabels['app.kubernetes.io/name']), 'app.kubernetes.io/name',
            isCCP == "True" and isnotnull(parsedPodLabels['overlay-app']), 'overlay-app',
            isCCP == "True" and isnotnull(parsedPodLabels['k8s-app']), 'k8s-app',
            isCCP == "True" and isnotnull(parsedPodLabels.app), 'app',
            isCCP == "False" and isnotnull(parsedPodLabels['name']), 'name',
            isCCP == "False" and isnotnull(parsedPodLabels['control-plane']), 'control-plane',
            isCCP == "False" and isnotnull(parsedPodLabels['app.kubernetes.io/name']), 'app.kubernetes.io/name',
            isCCP == "False" and isnotnull(parsedPodLabels['adxmon']), 'adxmon',
            isCCP == "False" and isnotnull(parsedPodLabels['k8s-app']), 'k8s-app',
            isCCP == "False" and isnotnull(parsedPodLabels.app), 'app',
            'other'
        )
        | extend 
            LabelValue = iif(LabelKey != 'other', parsedPodLabels[LabelKey], PodName),
            PodNamespace = iif(isCCP == "True", "CCP Namespace", PodNamespace)
        | distinct
            isCCP,
            LabelKey,
            LabelValue,
            PodNamespace
        | order by LabelKey
        | extend RowNum = row_number()
        """
        
        return query

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        before_sleep=log_retry_attempt,
        reraise=True
    )
    def execute_query_with_retries(self, database: str, query: str, chunk):
        """Execute a Kusto query with retries."""
        try:
            # Get formatted times for logging
            start_time = chunk[0].isoformat() if hasattr(chunk[0], 'isoformat') else chunk[0]
            end_time = chunk[1].isoformat() if hasattr(chunk[1], 'isoformat') else chunk[1]
            
            logger.info(
                f"Executing query for chunk {start_time} to {end_time}"
            )
            
            result = self.client.execute(database, query)
            
            logger.info(
                f"Query executed successfully for chunk {start_time} to {end_time}"
            )
            return result
                
        except Exception as e:
            logger.error(
                f"Query failed for chunk {start_time} to {end_time}: {str(e)}",
                exc_info=True  # This will include the full traceback in the log
            )
            raise  # Re-raise the exception to trigger retry

    def process_chunk(self, component: str, chunk: tuple[datetime, datetime]) -> Dict[str, Any]:
        """Process a single chunk with enhanced error tracking"""
        start_time, end_time = chunk
        chunk_info = {
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'component': component
        }

        try:
            query = self.build_query(component, start_time, end_time)
            # Pass the chunk parameter here
            response = self.execute_query_with_retries("AKSprod", query, chunk)  # Added chunk here
            results = [row.to_dict() for row in response.primary_results[0]]

            return {
                'success': True,
                'data': results,
                'chunk_info': chunk_info
            }
        except Exception as e:
            error_details = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': traceback.format_exc(),
                'chunk_info': chunk_info
            }
            return {
                'success': False,
                'error': error_details
            }

    def merge_results(self, all_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge results and ensure LabelKey-LabelValue pairs are unique."""
        unique_pairs = {}
        for result in all_results:
            key = result['LabelKey']
            value = result['LabelValue']
            is_ccp = result['isCCP']
            pod_namespace = result['PodNamespace']
            pair_key = (key, value, is_ccp, pod_namespace)
            if pair_key not in unique_pairs:
                unique_pairs[pair_key] = {
                    'LabelKey': key,
                    'LabelValue': value,
                    'isCCP': is_ccp,
                    'PodNamespace': pod_namespace
                }
        merged_results = [
            {
                'RowNum': idx + 1,
                **unique_pairs[pair_key]
            }
            for idx, pair_key in enumerate(sorted(unique_pairs.keys()))
        ]
        return merged_results

    def process_day(self, day_chunk: DayChunk, component: str, force_replace: bool = False) -> Optional[Dict[str, Any]]:
        """Process all chunks for a single day."""
        time_chunks = self.get_time_chunks(day_chunk)

        # Calculate expected number of chunks
        expected_chunks = int(24 * 60 / self.CHUNK_MINUTES)
        if len(time_chunks) != expected_chunks:
            click.echo(f"\nWarning: Expected {expected_chunks} chunks but got {len(time_chunks)} chunks")

        daily_results = []
        execution_info = {
            'date': day_chunk.date.isoformat(),
            'start_time': day_chunk.start_time.isoformat(),
            'end_time': day_chunk.end_time.isoformat(),
            'component': component,
            'errors': [],
            'successful_chunks': 0,
            'failed_chunks': 0,
            'total_chunks': len(time_chunks),
            'chunk_details': [
                {
                    'chunk_num': i+1,
                    'start': start.isoformat(),
                    'end': end.isoformat(),
                    'duration': str(end - start)
                }
                for i, (start, end) in enumerate(time_chunks)
            ]
        }

        click.echo(f"\nProcessing {component} for {day_chunk.date.isoformat()}...")

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            # Only submit chunks that don't exist in DB or if force_replace is True
            future_to_chunk = {}
            for chunk in time_chunks:
                chunk_period = ChunkPeriod(start_time=chunk[0], end_time=chunk[1])
                if force_replace or not self.pg_manager.check_chunk_exists(day_chunk.date, chunk_period, component):
                    future_to_chunk[executor.submit(self.process_chunk, component, chunk)] = chunk
                else:
                    logger.info(f"Skipping existing chunk {chunk[0].isoformat()} to {chunk[1].isoformat()}")
                    execution_info['successful_chunks'] += 1  # Count as successful since it exists

            # Process submitted chunks
            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    result = future.result()
                    if result['success']:
                        daily_results.extend(result['data'])
                        execution_info['successful_chunks'] += 1
                        # Store each successful chunk's results individually
                        self.pg_manager.store_chunk_results(
                            date=day_chunk.date,
                            chunk_period=ChunkPeriod(start_time=chunk[0], end_time=chunk[1]),
                            component=component,
                            results=result['data'],
                            execution_info={'success': True, 'chunk_info': result['chunk_info']}
                        )
                        logger.info(f"Chunk {chunk[0].isoformat()} to {chunk[1].isoformat()} processed successfully.")
                    else:
                        logger.error(f"Chunk {chunk[0].isoformat()} to {chunk[1].isoformat()} failed with error: {result['error']}")
                        execution_info['failed_chunks'] += 1
                        execution_info['errors'].append(result['error'])
                        return None
                except Exception as e:
                    execution_info['failed_chunks'] += 1
                    execution_info['errors'].append({
                        'error_type': type(e).__name__,
                        'error_message': str(e),
                        'traceback': traceback.format_exc(),
                        'chunk_info': {
                            'start_time': chunk[0].isoformat(),
                            'end_time': chunk[1].isoformat()
                        }
                    })
                    return None

        if daily_results:
            merged_results = self.merge_results(daily_results)
            return {
                'results': merged_results,
                'execution_info': execution_info
            }
        elif execution_info['successful_chunks'] == len(time_chunks):
            # All chunks were skipped because they exist
            return {
                'results': [],
                'execution_info': execution_info
            }
        return None

    def collect_and_store(self, component: str, force_replace: bool = False) -> None:
        """Collect traffic data and store in PostgreSQL by day."""
        day_chunks = self.get_day_chunks()

        for day_chunk in tqdm(day_chunks, desc="Processing days"):
            click.echo(f"\nProcessing date: {day_chunk.date}")

            # Process the day
            day_results = self.process_day(day_chunk, component, force_replace)
            
            # Print daily summary
            exec_info = day_results['execution_info']
            click.echo(f"Successfully processed {day_chunk.date}:")
            click.echo(f"Total results: {len(day_results['results'])}")
            click.echo(f"Successful chunks: {exec_info['successful_chunks']}")
            click.echo(f"Failed chunks: {exec_info['failed_chunks']}")
