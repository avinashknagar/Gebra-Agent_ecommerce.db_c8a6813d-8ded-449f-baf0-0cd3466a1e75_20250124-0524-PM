#!/usr/bin/env python3

import os
import sqlite3
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", "9200"))
DB_PATH = os.getenv("DB_PATH", "database.sqlite")

# Elasticsearch index mappings
INDEX_MAPPINGS = {
    "products": {
        "mappings": {
            "properties": {
                "id": {"type": "integer"