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


@dataclass
class ChunkPeriod:
    start_time: datetime
    end_time: datetime
    
    def __str__(self) -> str:
        return f"{self.start_time.isoformat()}_{self.end_time.isoformat()}"

