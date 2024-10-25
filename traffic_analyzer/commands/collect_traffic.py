# traffic_analyzer/commands/collect_traffic.py

import click
import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import traceback
import psycopg2
from psycopg2.extras import Json
import json
from dataclasses import dataclass
from datetime import datetime, time, timedelta

@dataclass
class DayChunk:
    date: datetime.date
    start_time: datetime
    end_time: datetime

class PostgresManager:
    def __init__(self, dbname="traffic_analyzer", user="postgres", host="localhost", port="5432"):
        password = click.prompt("Enter password for PostgreSQL user", hide_input=True)
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.init_database()

    def init_database(self):
        # Connect to default database to create our database if it doesn't exist
        conn = psycopg2.connect(**{**self.conn_params, "dbname": "postgres"})
        conn.autocommit = True
        cur = conn.cursor()
        
        # Create database if it doesn't exist
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (self.conn_params["dbname"],))
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {self.conn_params['dbname']}")
        
        cur.close()
        conn.close()

        # Connect to our database and create table
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS msi_connector (
                        dateonly DATE PRIMARY KEY,
                        component VARCHAR(255) NOT NULL,
                        results JSONB NOT NULL,
                        execution_info JSONB NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.commit()

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def check_date_exists(self, date: datetime.date, component: str) -> bool:
        """Check if data exists for a specific date and component."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM msi_connector 
                    WHERE dateonly = %s AND component = %s
                """, (date, component))
                return cur.fetchone() is not None

    def store_daily_results(self, date: datetime.date, component: str, results: Dict, execution_info: Dict):
        """Store results for a day, replacing any existing data."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO msi_connector (dateonly, component, results, execution_info)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (dateonly) 
                    DO UPDATE SET
                        results = EXCLUDED.results,
                        execution_info = EXCLUDED.execution_info,
                        updated_at = CURRENT_TIMESTAMP
                """, (date, component, Json(results), Json(execution_info)))
                conn.commit()

class TrafficCollector:
    def __init__(self, cluster_url="https://akshuba.centralus.kusto.windows.net",
                 days_to_collect=2, chunk_hours=1, max_workers=16):
        self.cluster_url = cluster_url
        self.DAYS_TO_COLLECT = days_to_collect
        self.CHUNK_HOURS = chunk_hours
        self.MAX_WORKERS = max_workers
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_url)
        self.client = KustoClient(kcsb)
        self.pg_manager = PostgresManager()

    def get_day_chunks(self) -> List[DayChunk]:
        """Generate time chunks aligned to full days."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=self.DAYS_TO_COLLECT)
        
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
        """Generate hourly chunks for a specific day."""
        chunks = []
        current = day_chunk.start_time
        
        # Calculate number of chunks needed for the day
        total_seconds = (day_chunk.end_time - day_chunk.start_time).total_seconds()
        chunk_seconds = self.CHUNK_HOURS * 3600
        num_chunks = int((total_seconds + chunk_seconds - 1) // chunk_seconds)
        
        for _ in range(num_chunks):
            chunk_end = min(current + timedelta(hours=self.CHUNK_HOURS), day_chunk.end_time)
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

    def build_query(self, component: str, start_time: datetime,
                    end_time: datetime) -> str:
        """Build Kusto query for a specific time chunk."""
        query = f"""
        let endTime = datetime('{end_time.isoformat()}');
        let startTime = datetime('{start_time.isoformat()}');
        let targetComponent = '{component}';

        // Extract traffic data for the target component
        let msiConnectorTraffic =
        cluster('{self.cluster_url}').database('AKSprod').IncomingRequestTrace
        | where TIMESTAMP between (startTime .. endTime)
        | where namespace == targetComponent
        | extend clientIP = tostring(split(clientRemoteAddr, ':')[0])
        | project clientIP, UnderlayName
        | distinct clientIP, UnderlayName;

        // Get the latest Pod information
        let podInfo =
        cluster('{self.cluster_url}').database('AKSinfra').ProcessInfo
        | where TIMESTAMP between (startTime .. endTime)
        | summarize arg_max(TIMESTAMP, PodLabels, PodName, PodNamespace) by PodIP, UnderlayName;

        // Join traffic data with Pod information
        msiConnectorTraffic
        | join kind=inner hint.strategy=broadcast (podInfo) on UnderlayName, $left.clientIP == $right.PodIP
        | where PodNamespace matches regex "^[0-9]"
        | extend PodLabels = todynamic(PodLabels)
        | extend LabelKey = case(
            isnotnull(PodLabels['kube-egress-gateway-control-plane']), 'kube-egress-gateway-control-plane',
            isnotnull(PodLabels['rsName']), 'rsName',
            isnotnull(PodLabels['control-plane']), 'control-plane',
            isnotnull(PodLabels['app.kubernetes.io/name']), 'app.kubernetes.io/name',
            isnotnull(PodLabels['overlay-app']), 'overlay-app',
            isnotnull(PodLabels['k8s-app']), 'k8s-app',
            isnotnull(PodLabels.app), 'app',
            'other'
        )
        | extend LabelValue = iif(LabelKey != 'other', PodLabels[LabelKey], PodName)
        | distinct LabelKey, LabelValue
        """
        return query

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10),
           retry=retry_if_exception_type(Exception))
    def execute_query_with_retries(self, database: str, query: str):
        """Execute a Kusto query with retries."""
        return self.client.execute(database, query)

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
            response = self.execute_query_with_retries("AKSprod", query)
            results = response.primary_results[0] if response.primary_results[0] else []
            
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
            pair_key = (key, value)
            if pair_key not in unique_pairs:
                unique_pairs[pair_key] = True
                
        merged_results = [
            {
                'RowNum': idx + 1,
                'LabelKey': key,
                'LabelValue': value
            }
            for idx, (key, value) in enumerate(sorted(unique_pairs.keys()))
        ]
        return merged_results

    def process_day(self, day_chunk: DayChunk, component: str) -> Optional[Dict[str, Any]]:
        """Process all chunks for a single day."""
        time_chunks = self.get_time_chunks(day_chunk)
        
        # Validate we have the right number of chunks
        expected_chunks = 24 // self.CHUNK_HOURS
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
            future_to_chunk = {
                executor.submit(self.process_chunk, component, chunk): chunk
                for chunk in time_chunks
            }

            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    result = future.result()
                    if result['success']:
                        daily_results.extend(result['data'])
                        execution_info['successful_chunks'] += 1
                    else:
                        execution_info['failed_chunks'] += 1
                        execution_info['errors'].append(result['error'])
                        # If any chunk fails, return None to skip the entire day
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
                    # If any chunk fails, return None to skip the entire day
                    return None

        if daily_results:
            merged_results = self.merge_results(daily_results)
            return {
                'results': merged_results,
                'execution_info': execution_info
            }
        return None

    def collect_and_store(self, component: str, force_replace: bool = False) -> None:
        """Collect traffic data and store in PostgreSQL by day."""
        day_chunks = self.get_day_chunks()
        
        for day_chunk in tqdm(day_chunks, desc="Processing days"):
            click.echo(f"\nProcessing date: {day_chunk.date}")
            
            # Skip if data exists and force_replace is False
            if not force_replace and self.pg_manager.check_date_exists(day_chunk.date, component):
                click.echo(f"Data already exists for {day_chunk.date}, skipping...")
                continue
            
            # Process the day
            day_results = self.process_day(day_chunk, component)
            
            if day_results:
                # Store in PostgreSQL
                self.pg_manager.store_daily_results(
                    date=day_chunk.date,
                    component=component,
                    results=day_results['results'],
                    execution_info=day_results['execution_info']
                )
                
                # Print daily summary
                exec_info = day_results['execution_info']
                click.echo(f"Successfully processed {day_chunk.date}:")
                click.echo(f"Total results: {len(day_results['results'])}")
                click.echo(f"Successful chunks: {exec_info['successful_chunks']}")
                click.echo(f"Failed chunks: {exec_info['failed_chunks']}")
            else:
                click.echo(f"Failed to process {day_chunk.date} - skipping storage")