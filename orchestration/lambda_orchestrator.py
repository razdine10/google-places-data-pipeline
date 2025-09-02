import json
import boto3
import subprocess
import os
from datetime import datetime
import logging

# Logging configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Main orchestrator for the data pipeline.
    Pipeline: Ingestion → dbt → Validation
    """
    logger.info("Starting pipeline orchestration")

    pipeline_status = {
        "pipeline_id": f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "start_time": datetime.now().isoformat(),
        "steps": []
    }

    try:
        # Step 1: Data ingestion
        logger.info("STEP 1: Data ingestion")
        ingestion_result = run_data_ingestion()
        pipeline_status["steps"].append({
            "step": "ingestion",
            "status": "success" if ingestion_result["success"] else "failed",
            "message": ingestion_result["message"],
            "duration": ingestion_result.get("duration", 0)
        })

        if not ingestion_result["success"]:
            raise Exception(f"Ingestion failed: {ingestion_result['message']}")

        # Step 2: dbt transformation
        logger.info("STEP 2: dbt transformation")
        dbt_result = run_dbt_pipeline()
        pipeline_status["steps"].append({
            "step": "dbt_transformation",
            "status": "success" if dbt_result["success"] else "failed",
            "message": dbt_result["message"],
            "duration": dbt_result.get("duration", 0)
        })

        if not dbt_result["success"]:
            raise Exception(f"dbt failed: {dbt_result['message']}")

        # Step 3: Data quality tests
        logger.info("STEP 3: Data quality tests")
        test_result = run_data_quality_tests()
        pipeline_status["steps"].append({
            "step": "data_quality_tests",
            "status": "success" if test_result["success"] else "failed",
            "message": test_result["message"],
            "duration": test_result.get("duration", 0)
        })

        # Step 4: Notification
        logger.info("STEP 4: Notification")
        send_notification(pipeline_status, "SUCCESS")

        pipeline_status["end_time"] = datetime.now().isoformat()
        pipeline_status["overall_status"] = "SUCCESS"

        logger.info("Pipeline completed successfully")

        return {
            'statusCode': 200,
            'body': json.dumps(pipeline_status)
        }

    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")

        pipeline_status["end_time"] = datetime.now().isoformat()
        pipeline_status["overall_status"] = "FAILED"
        pipeline_status["error"] = str(e)

        send_notification(pipeline_status, "FAILED")

        return {
            'statusCode': 500,
            'body': json.dumps(pipeline_status)
        }

def run_data_ingestion():
    """Trigger data collection via Lambda."""
    start_time = datetime.now()

    try:
        logger.info("Triggering Google Places collection Lambda...")

        lambda_client = boto3.client('lambda')
        response = lambda_client.invoke(
            FunctionName='google-places-collector',
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "source": "orchestrator",
                "max_restaurants": 20
            })
        )

        result = json.loads(response['Payload'].read())

        if response['StatusCode'] != 200:
            raise Exception(f"Collection Lambda error: {result}")

        duration = (datetime.now() - start_time).total_seconds()
        return {
            "success": True,
            "message": "Ingestion succeeded",
            "duration": duration,
            "details": result
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        return {
            "success": False,
            "message": f"Ingestion error: {str(e)}",
            "duration": duration
        }

def run_dbt_pipeline():
    """Run dbt transformations."""
    start_time = datetime.now()

    try:
        logger.info("Running dbt transformations...")

        dbt_commands = [
            "dbt deps",
            "dbt seed",
            "dbt run",
            "dbt test"
        ]

        results = []
        os.chdir('/opt/reviewflow_dbt')

        for command in dbt_commands:
            logger.info(f"Executing: {command}")
            result = subprocess.run(
                command.split(),
                capture_output=True,
                text=True,
                timeout=300
            )
            if result.returncode != 0:
                raise Exception(f"Failed {command}: {result.stderr}")
            results.append({
                "command": command,
                "status": "success",
                "output": result.stdout[:500]
            })

        duration = (datetime.now() - start_time).total_seconds()
        return {
            "success": True,
            "message": "dbt transformations succeeded",
            "duration": duration,
            "details": results
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        return {
            "success": False,
            "message": f"dbt error: {str(e)}",
            "duration": duration
        }

def run_data_quality_tests():
    """Run additional data quality checks."""
    start_time = datetime.now()

    try:
        logger.info("Running data quality tests...")

        import duckdb
        conn = duckdb.connect('/opt/reviewflow_dbt/reviewflow.duckdb')

        restaurant_count = conn.execute("SELECT COUNT(*) FROM mart_top_restaurants").fetchone()[0]
        if restaurant_count == 0:
            raise Exception("No restaurants found in mart_top_restaurants")

        sentiment_issues = conn.execute(
            """
            SELECT COUNT(*)
            FROM stg_reviews
            WHERE (rating >= 4 AND sentiment_simple != 'Positive')
               OR (rating <= 2 AND sentiment_simple != 'Negative')
            """
        ).fetchone()[0]

        tests_results = {
            "restaurant_count": restaurant_count,
            "sentiment_consistency_issues": sentiment_issues
        }
        conn.close()

        duration = (datetime.now() - start_time).total_seconds()
        return {
            "success": True,
            "message": f"Quality tests passed - {restaurant_count} restaurants processed",
            "duration": duration,
            "details": tests_results
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        return {
            "success": False,
            "message": f"Quality tests error: {str(e)}",
            "duration": duration
        }

def send_notification(pipeline_status, status):
    """Send pipeline status notification via SNS and optional Slack webhook."""
    try:
        sns = boto3.client('sns')
        topic_arn = os.environ.get('SNS_TOPIC_ARN')

        if topic_arn:
            message = f"""
Pipeline Status: {status}

Pipeline ID: {pipeline_status['pipeline_id']}
Start: {pipeline_status['start_time']}
End: {pipeline_status.get('end_time', 'In progress')}

Steps:
"""
            for step in pipeline_status['steps']:
                message += f"- {step['step']}: {step['status']} ({step.get('duration', 0):.1f}s)\n"

            if status == "FAILED":
                message += f"\nError: {pipeline_status.get('error', 'Unknown error')}"

            sns.publish(
                TopicArn=topic_arn,
                Subject=f"Data Pipeline - {status}",
                Message=message
            )

        webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
        if webhook_url:
            import requests
            color = "good" if status == "SUCCESS" else "danger"
            slack_message = {
                "attachments": [{
                    "color": color,
                    "title": f"Data Pipeline - {status}",
                    "text": f"Pipeline {pipeline_status['pipeline_id']} completed",
                    "fields": [
                        {"title": "Status", "value": status, "short": True},
                        {"title": "Steps", "value": len(pipeline_status['steps']), "short": True}
                    ]
                }]
            }
            requests.post(webhook_url, json=slack_message)

        logger.info(f"Notification {status} sent")

    except Exception as e:
        logger.error(f"Notification error: {str(e)}") 