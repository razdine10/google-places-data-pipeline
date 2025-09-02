import json
import boto3
import os
import logging
from datetime import datetime

# Import existing collector
import sys
sys.path.append('/opt')
from google_places_collector import GooglePlacesCollector

# Logging configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Google Places data collection Lambda.
    Triggered by the orchestrator or a schedule.
    """
    logger.info("Starting data collection")

    try:
        # Collection parameters
        max_restaurants = event.get('max_restaurants', 20)
        target_city = event.get('target_city', 'Paris')
        source = event.get('source', 'manual')

        logger.info(f"Collecting for {target_city} - max {max_restaurants} restaurants")

        # Initialize collector
        collector = GooglePlacesCollector()

        # Override parameters if provided
        if max_restaurants:
            collector.max_restaurants = max_restaurants
        if target_city:
            collector.target_city = target_city

        # Run collection
        start_time = datetime.now()
        collector.run()
        end_time = datetime.now()

        duration = (end_time - start_time).total_seconds()

        stats = {
            "success": True,
            "source": source,
            "target_city": target_city,
            "max_restaurants": max_restaurants,
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "message": "Collection completed successfully"
        }

        logger.info(f"Collection completed in {duration:.1f}s")

        # Optionally trigger next step
        if source == "orchestrator":
            pass
        elif event.get('trigger_next_step', False):
            trigger_orchestrator(stats)

        return {
            'statusCode': 200,
            'body': json.dumps(stats)
        }

    except Exception as e:
        logger.error(f"Error during collection: {str(e)}")

        error_stats = {
            "success": False,
            "source": event.get('source', 'manual'),
            "error": str(e),
            "end_time": datetime.now().isoformat(),
            "message": f"Collection failed: {str(e)}"
        }

        return {
            'statusCode': 500,
            'body': json.dumps(error_stats)
        }

def trigger_orchestrator(collection_stats):
    """
    Trigger the orchestrator after a successful collection.
    """
    try:
        lambda_client = boto3.client('lambda')

        payload = {
            "trigger_source": "data_collection",
            "collection_stats": collection_stats
        }

        lambda_client.invoke(
            FunctionName='data-pipeline-orchestrator',
            InvocationType='Event',  # async
            Payload=json.dumps(payload)
        )

        logger.info("Orchestrator triggered after collection")

    except Exception as e:
        logger.error(f"Error triggering orchestrator: {str(e)}") 