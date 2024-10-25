import click
import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import traceback

@click.group()
def cli():
    pass

class TrafficCollector:
    def __init__(self, cluster_url="https://akshuba.centralus.kusto.windows.net",
                 days_to_collect=2, chunk_hours=1, max_workers=16):
        self.cluster_url = cluster_url
        self.DAYS_TO_COLLECT = days_to_collect
        self.CHUNK_HOURS = chunk_hours
        self.MAX_WORKERS = max_workers
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_url)
        self.client = KustoClient(kcsb)

    def get_time_chunks(self) -> List[tuple]:
        """Generate time chunks based on provided days and hours."""
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(days=self.DAYS_TO_COLLECT)
        chunks = []
        current_start = start_time
        while current_start < end_time:
            chunk_end = min(current_start + datetime.timedelta(hours=self.CHUNK_HOURS), end_time)
            chunks.append((current_start, chunk_end))
            current_start = chunk_end
        return chunks

    def build_query(self, component: str, start_time: datetime.datetime,
                    end_time: datetime.datetime) -> str:
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
            response = self.client.execute("AKSprod", query)
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

    def collect_traffic(self, component: str) -> List[Dict[str, Any]]:
        """Collect traffic data with enhanced error handling and debugging"""
        execution_info = {
            'start_time': datetime.datetime.utcnow().isoformat(),
            'component': component,
            'errors': [],
            'successful_chunks': 0,
            'failed_chunks': 0
        }
        
        try:
            chunks = self.get_time_chunks()
            all_results = []
            execution_info['total_chunks'] = len(chunks)

            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                future_to_chunk = {
                    executor.submit(self.process_chunk, component, chunk): chunk
                    for chunk in chunks
                }

                with tqdm(total=len(chunks), desc="Processing chunks") as pbar:
                    for future in as_completed(future_to_chunk):
                        chunk = future_to_chunk[future]
                        try:
                            result = future.result()
                            
                            if result['success']:
                                all_results.extend(result['data'])
                                execution_info['successful_chunks'] += 1
                            else:
                                execution_info['failed_chunks'] += 1
                                execution_info['errors'].append(result['error'])
                                
                                # Print detailed error information
                                error_details = result['error']
                                click.echo(click.style(f"\nError in chunk {error_details['chunk_info']}", fg='red'))
                                click.echo(f"Error Type: {error_details['error_type']}")
                                click.echo(f"Error Message: {error_details['error_message']}")
                                click.echo("Traceback:")
                                click.echo(error_details['traceback'])
                                
                        except Exception as e:
                            execution_info['failed_chunks'] += 1
                            error_info = {
                                'error_type': type(e).__name__,
                                'error_message': str(e),
                                'traceback': traceback.format_exc(),
                                'chunk_info': {
                                    'start_time': chunk[0].isoformat(),
                                    'end_time': chunk[1].isoformat()
                                }
                            }
                            execution_info['errors'].append(error_info)
                            click.echo(f"\nUnexpected error in future execution:", err=True)
                            click.echo(f"Error Type: {type(e).__name__}")
                            click.echo(f"Error Message: {str(e)}")
                            click.echo("Traceback:")
                            click.echo(traceback.format_exc())
                            
                        pbar.update(1)

            execution_info['end_time'] = datetime.datetime.utcnow().isoformat()
            execution_info['total_results'] = len(all_results)

            # Print execution summary
            click.echo("\nExecution Summary:")
            click.echo("-" * 40)
            click.echo(f"Total Chunks: {execution_info['total_chunks']}")
            click.echo(f"Successful Chunks: {execution_info['successful_chunks']}")
            click.echo(f"Failed Chunks: {execution_info['failed_chunks']}")
            click.echo(f"Total Results: {execution_info['total_results']}")
            
            if execution_info['errors']:
                click.echo("\nError Summary:")
                click.echo("-" * 40)
                for i, error in enumerate(execution_info['errors'], 1):
                    click.echo(f"\nError {i}:")
                    click.echo(f"Type: {error['error_type']}")
                    click.echo(f"Message: {error['error_message']}")
                    click.echo(f"Chunk: {error['chunk_info']}")

            if all_results:
                merged_results = self.merge_results(all_results)
                return {
                    'results': merged_results,
                    'execution_info': execution_info
                }
            return {
                'results': None,
                'execution_info': execution_info
            }

        except Exception as e:
            execution_info['end_time'] = datetime.datetime.utcnow().isoformat()
            execution_info['fatal_error'] = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': traceback.format_exc()
            }
            
            click.echo("\nFatal Error:", err=True)
            click.echo(f"Error Type: {type(e).__name__}")
            click.echo(f"Error Message: {str(e)}")
            click.echo("Traceback:")
            click.echo(traceback.format_exc())
            
            return {
                'results': None,
                'execution_info': execution_info
            }
