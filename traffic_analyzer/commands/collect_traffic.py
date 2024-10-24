import click
import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.identity import DefaultAzureCredential
from typing import List, Dict, Any


class TrafficCollector:
    # Constants for time management
    DAYS_TO_COLLECT = 1
    CHUNK_HOURS = 6

    def __init__(self, cluster_url="https://akshuba.centralus.kusto.windows.net"):
        self.cluster_url = cluster_url
        self.credential = DefaultAzureCredential()
        self.kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(
            cluster_url)
        self.client = KustoClient(self.kcsb)

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

    def merge_results(self, all_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge and deduplicate results from all chunks"""
        # Convert list of results to a set of tuples for deduplication
        unique_results = set()
        for result in all_results:
            unique_results.add((result['LabelKey'], result['LabelValue']))

        # Convert back to list of dicts with row numbers
        merged_results = [
            {
                'RowNum': idx + 1,
                'LabelKey': key,
                'LabelValue': value
            }
            for idx, (key, value) in enumerate(sorted(unique_results))
        ]

        return merged_results

    def collect_traffic(self, component: str) -> List[Dict[str, Any]]:
        """Collect traffic data for all time chunks and merge results"""
        try:
            all_results = []
            chunks = self.get_time_chunks()
            total_chunks = len(chunks)

            with click.progressbar(chunks, label='Collecting traffic data',
                                   length=total_chunks) as chunks_bar:
                for start_time, end_time in chunks_bar:
                    query = self.build_query(component, start_time, end_time)
                    response = self.client.execute("AKSprod", query)
                    if response.primary_results[0]:
                        all_results.extend(response.primary_results[0])

            if all_results:
                return self.merge_results(all_results)
            return None

        except KustoServiceError as e:
            click.echo(f"Error querying Kusto: {str(e)}", err=True)
            return None
        except Exception as e:
            click.echo(f"Unexpected error: {str(e)}", err=True)
            return None
