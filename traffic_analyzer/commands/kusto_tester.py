# traffic_analyzer/commands/test_kusto.py

import click
import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.identity import DefaultAzureCredential
from typing import Dict, Any
import json


class KustoTester:
    def __init__(self, cluster_url="https://akshuba.centralus.kusto.windows.net"):
        self.cluster_url = cluster_url
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_url)
        self.client = KustoClient(kcsb)


    def test_raw_query(self, component: str) -> Dict[str, Any]:
        """Test raw kusto query with single result"""
        try:
            query = f"""
            cluster('{self.cluster_url}').database('AKSprod').IncomingRequestTrace
            | where namespace == '{component}'
            | take 1
            """
            
            response = self.client.execute("AKSprod", query)
            
            if response.primary_results[0]:
                return {
                    'success': True,
                    'results': response.primary_results[0],
                    'row_count': len(response.primary_results[0])
                }
            return {
                'success': False,
                'error': 'No results found',
                'row_count': 0
            }

        except KustoServiceError as e:
            return {
                'success': False,
                'error': f"Kusto query error: {str(e)}",
                'exception_type': 'KustoServiceError'
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Unexpected error: {str(e)}",
                'exception_type': type(e).__name__
            }

