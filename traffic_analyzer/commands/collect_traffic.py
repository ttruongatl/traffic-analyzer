import click
import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.identity import DefaultAzureCredential
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

class TrafficCollector:
    # Constants for time management
    DAYS_TO_COLLECT = 15
    CHUNK_HOURS = 6
    MAX_WORKERS = 16  # Limit parallel requests

    def __init__(self, cluster_url="https://akshuba.centralus.kusto.windows.net"):
        self.cluster_url = cluster_url
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_url)
        self.client = KustoClient(kcsb)


    def get_time_chunks(self) -> List[tuple]:
        """Generate time chunks for the last 30 days in 6-hour intervals"""
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(days=self.DAYS_TO_COLLECT)

        chunks = []
        current_start = start_time

        while current_start < end_time:
            chunk_end = min(
                current_start + datetime.timedelta(hours=self.CHUNK_HOURS), end_time)
            chunks.append((current_start, chunk_end))
            current_start = chunk_end

        return chunks

    def build_query(self, component: str, start_time: datetime.datetime,
                    end_time: datetime.datetime) -> str:
        """Build Kusto query for a specific time chunk"""
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
        | project clientIP, UnderlayName, TIMESTAMP
        | summarize by clientIP, UnderlayName, bin(TIMESTAMP, 1h);
        
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

    def process_chunk(self, component: str, time_chunk: tuple) -> List[Dict[str, Any]]:
        """Process a single time chunk and return results"""
        start_time, end_time = time_chunk
        try:
            query = self.build_query(component, start_time, end_time)
            click.echo(f"Start processing chunk {start_time} - {end_time}")
            click.echo(f"Query: {query}")
            response = self.client.execute("AKSprod", query)
            click.echo(f"Finished processing chunk {start_time} - {end_time}")
            return response.primary_results[0] if response.primary_results[0] else []
        except Exception as e:
            click.echo(f"Error processing chunk {start_time} - {end_time}: {str(e)}", err=True)
            return []

    def merge_results(self, all_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge results and ensure LabelKey-LabelValue pairs are unique"""
        # Create a dictionary to track unique pairs
        unique_pairs = {}
        
        for result in all_results:
            key = result['LabelKey']
            value = result['LabelValue']
            pair_key = (key, value)
            
            # Only add if we haven't seen this exact key-value pair before
            if pair_key not in unique_pairs:
                unique_pairs[pair_key] = True
        
        # Convert to final format with row numbers
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
        """Collect traffic data for all time chunks in parallel and merge results"""
        try:
            chunks = self.get_time_chunks()
            all_results = []

            # Using ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                # Submit all tasks
                future_to_chunk = {
                    executor.submit(self.process_chunk, component, chunk): chunk
                    for chunk in chunks
                }

                # Process completed tasks with progress bar
                with tqdm(total=len(chunks), desc="Processing chunks") as pbar:
                    for future in as_completed(future_to_chunk):
                        chunk = future_to_chunk[future]
                        try:
                            chunk_results = future.result()
                            if chunk_results:
                                # Each chunk result is already unique from the Kusto query
                                all_results.extend(chunk_results)
                        except Exception as e:
                            click.echo(f"Chunk {chunk} generated an exception: {str(e)}", err=True)
                        pbar.update(1)

            if all_results:
                # Final deduplication happens here
                return self.merge_results(all_results)
            return None

        except Exception as e:
            click.echo(f"Unexpected error: {str(e)}", err=True)
            return None