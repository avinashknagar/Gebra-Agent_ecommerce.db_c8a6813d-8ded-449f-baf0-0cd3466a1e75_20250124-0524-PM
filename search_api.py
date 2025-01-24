import os
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from elasticsearch import Elasticsearch, RequestError, ConnectionError
from pydantic import BaseModel
from dotenv import load_dotenv
import math

# Load environment variables
load_dotenv()

# Configure environment variables with defaults
API_PORT = int(os.getenv('API_PORT', '8001'))
ES_HOST = os.getenv('ES_HOST', 'localhost')
ES_PORT = os.getenv('ES_PORT', '9200')
ES_SCHEME = os.getenv('ES_SCHEME', 'http')
ES_INDEX = os.getenv('ES_INDEX')
ES_USER = os.getenv('ES_USER')
ES_PASSWORD = os.getenv('ES_PASSWORD')

# Initialize FastAPI app
app = FastAPI(
    title='Search API',
    description='API for searching Elasticsearch index',
    version='1.0.0'
)

# Response model for search results
class SearchResult(BaseModel):
    total: int
    hits: List[Dict[str, Any]]
    page: int
    page_size: int
    total_pages: int

# Initialize Elasticsearch client
def get_elasticsearch_client() -> Elasticsearch:
    try:
        es_client = Elasticsearch(
            f'{ES_SCHEME}://{ES_HOST}:{ES_PORT}',
            basic_auth=(ES_USER, ES_PASSWORD) if ES_USER and ES_PASSWORD else None,
            verify_certs=ES_SCHEME == 'https'
        )
        if not es_client.ping():
            raise ConnectionError('Failed to connect to Elasticsearch')
        return es_client
    except Exception as e:
        raise ConnectionError(f'Failed to initialize Elasticsearch client: {str(e)}')

@app.get('/search', response_model=SearchResult)
async def search(
    query: str = Query(..., description='Search query string'),
    page: int = Query(1, ge=1, description='Page number'),
    page_size: int = Query(10, ge=1, le=100, description='Number of results per page')
) -> SearchResult:
    """Search endpoint that performs fuzzy and wildcard searches across the index."""
    try:
        # Calculate offset for pagination
        from_idx = (page - 1) * page_size

        # Initialize Elasticsearch client
        es_client = get_elasticsearch_client()

        # Construct search query
        search_query = {
            'query': {
                'bool': {
                    'should': [
                        # Fuzzy search on text fields
                        {
                            'multi_match': {
                                'query': query,
                                'fields': ['name^3', 'description^2', 'category.name', 'tags'],
                                'fuzziness': 'AUTO'
                            }
                        },
                        # Wildcard search
                        {
                            'multi_match': {
                                'query': f'*{query}*',
                                'fields': ['name^3', 'description^2', 'category.name', 'tags'],
                                'type': 'phrase_prefix'
                            }
                        }
                    ],
                    'minimum_should_match': 1
                }
            },
            'from': from_idx,
            'size': page_size,
            'highlight': {
                'fields': {
                    'name': {},
                    'description': {}
                }
            }
        }

        # Execute search
        response = es_client.search(
            index=ES_INDEX,
            body=search_query
        )

        # Process results
        total_hits = response['hits']['total']['value']
        total_pages = math.ceil(total_hits / page_size)

        # Format hits
        formatted_hits = [
            {
                'id': hit['_id'],
                'score': hit['_score'],
                'source': hit['_source'],
                'highlights': hit.get('highlight', {})
            }
            for hit in response['hits']['hits']
        ]

        return SearchResult(
            total=total_hits,
            hits=formatted_hits,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )

    except ConnectionError as e:
        raise HTTPException(
            status_code=503,
            detail=f'Elasticsearch connection error: {str(e)}'
        )
    except RequestError as e:
        raise HTTPException(
            status_code=400,
            detail=f'Invalid search query: {str(e)}'
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Internal server error: {str(e)}'
        )

@app.on_event('startup')
async def startup_event():
    """Verify Elasticsearch connection and index existence on startup."""
    if not ES_INDEX:
        raise ValueError('ES_INDEX environment variable must be set')
    
    try:
        es_client = get_elasticsearch_client()
        if not es_client.indices.exists(index=ES_INDEX):
            raise ValueError(f'Index {ES_INDEX} does not exist')
    except Exception as e:
        raise RuntimeError(f'Failed to initialize API: {str(e)}')

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=API_PORT)
