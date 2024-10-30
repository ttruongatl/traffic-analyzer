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
from ..models.chunk import DayChunk, ChunkPeriod

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

        # Connect to our database and create table with chunk period as part of primary key
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS component_dependency_chunks (
                        dateonly DATE NOT NULL,
                        chunk_start TIMESTAMP WITH TIME ZONE NOT NULL,
                        chunk_end TIMESTAMP WITH TIME ZONE NOT NULL,
                        component VARCHAR(255) NOT NULL,
                        results JSONB NOT NULL,
                        execution_info JSONB NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (dateonly, chunk_start, chunk_end, component)
                    )
                """)
                
                # Add index for efficient date range queries
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_component_dependency_chunks_date_component 
                    ON component_dependency_chunks (dateonly, component)
                """)
                conn.commit()

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def check_chunk_exists(self, date: datetime.date, chunk_period: ChunkPeriod, component: str) -> bool:
        """Check if data exists for a specific chunk period and component."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM component_dependency_chunks 
                    WHERE dateonly = %s 
                    AND chunk_start = %s 
                    AND chunk_end = %s 
                    AND component = %s
                """, (date, chunk_period.start_time, chunk_period.end_time, component))
                return cur.fetchone() is not None

    def store_chunk_results(self, date: datetime.date, chunk_period: ChunkPeriod, 
                      component: str, results: Dict, execution_info: Dict):
        """Store results for a specific chunk period, replacing any existing data."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO component_dependency_chunks 
                    (dateonly, chunk_start, chunk_end, component, results, execution_info)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (dateonly, chunk_start, chunk_end, component) 
                    DO UPDATE SET
                        results = EXCLUDED.results,
                        execution_info = EXCLUDED.execution_info,
                        updated_at = CURRENT_TIMESTAMP
                """, (date, chunk_period.start_time, chunk_period.end_time, 
                    component, Json(results), Json(execution_info)))
                conn.commit()

    
    def get_results_for_date_range(self, start_date: datetime.date, 
                                 end_date: datetime.date, component: str) -> List[Dict]:
        """Retrieve results for a date range and component."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT results FROM component_dependency_chunks
                    WHERE dateonly BETWEEN %s AND %s 
                    AND component = %s
                    ORDER BY dateonly, chunk_start
                """, (start_date, end_date, component))
                rows = cur.fetchall()
                results = []
                for row in rows:
                    results.extend(row[0])  # 'results' column contains the data
                return results