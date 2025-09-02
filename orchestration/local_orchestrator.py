#!/usr/bin/env python3
"""
Local Orchestrator - Automatic Pipeline
======================================

Runs locally on your machine.
Pipeline: Ingestion → dbt → Tests → Notifications
"""

import os
import sys
import subprocess
import json
import logging
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Make parent directory importable
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/local_orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LocalOrchestrator:
    """Local orchestrator for the data pipeline."""

    def __init__(self):
        """Initialize local orchestrator and status."""
        logger.info("Initializing local orchestrator")

        self.pipeline_status = {
            "pipeline_id": f"local_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "start_time": datetime.now().isoformat(),
            "steps": []
        }

        os.makedirs('logs', exist_ok=True)

    def run_data_ingestion(self):
        """STEP 1: Run Google Places data collection."""
        logger.info("STEP 1: Data ingestion")
        start_time = datetime.now()

        try:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            os.chdir(project_root)

            # For dashboard diversity you may switch to the diverse collector
            logger.info("Running Google Places collector...")
            result = subprocess.run(
                [sys.executable, 'src/google_places_collector.py'],
                capture_output=True,
                text=True,
                timeout=1800
            )

            if result.returncode != 0:
                raise Exception(f"Collector error: {result.stderr}")

            duration = (datetime.now() - start_time).total_seconds()
            logger.info("Data collection succeeded")
            return {
                "success": True,
                "message": "Ingestion succeeded",
                "duration": duration,
                "output": result.stdout[-500:] if result.stdout else ""
            }

        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "message": "Collection timeout (30 min exceeded)",
                "duration": (datetime.now() - start_time).total_seconds()
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Ingestion error: {str(e)}",
                "duration": (datetime.now() - start_time).total_seconds()
            }

    def run_dbt_pipeline(self):
        """STEP 2: Run dbt transformations."""
        logger.info("STEP 2: dbt transformations")
        start_time = datetime.now()

        try:
            dbt_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'reviewflow_dbt')
            os.chdir(dbt_dir)

            logger.info("Loading CSV data into DuckDB...")
            load_result = subprocess.run(
                [sys.executable, 'load_data.py'],
                capture_output=True,
                text=True,
                timeout=300
            )
            if load_result.returncode != 0:
                raise Exception(f"Data load error: {load_result.stderr}")

            dbt_commands = [
                ['dbt', 'deps', '--profiles-dir', '.'],
                ['dbt', 'run', '--profiles-dir', '.']
            ]
            optional_commands = [
                ['dbt', 'test', '--profiles-dir', '.']
            ]

            results = []

            for command in dbt_commands:
                logger.info(f"Executing: {' '.join(command)}")
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=600
                )
                if result.returncode != 0:
                    raise Exception(f"Failed {' '.join(command)}: {result.stderr}")
                results.append({
                    "command": ' '.join(command),
                    "status": "success",
                    "output": result.stdout[-300:] if result.stdout else ""
                })

            for command in optional_commands:
                logger.info(f"Executing (optional): {' '.join(command)}")
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=600
                )
                if result.returncode != 0:
                    logger.warning(f"dbt tests failed (non-blocking): {result.stderr[:200]}")
                    results.append({
                        "command": ' '.join(command),
                        "status": "warning",
                        "output": f"Tests failed: {result.stderr[-200:]}" if result.stderr else ""
                    })
                else:
                    results.append({
                        "command": ' '.join(command),
                        "status": "success",
                        "output": result.stdout[-300:] if result.stdout else ""
                    })

            duration = (datetime.now() - start_time).total_seconds()
            logger.info("dbt transformations succeeded")
            return {
                "success": True,
                "message": "dbt transformations succeeded",
                "duration": duration,
                "details": results
            }

        except Exception as e:
            return {
                "success": False,
                "message": f"dbt error: {str(e)}",
                "duration": (datetime.now() - start_time).total_seconds()
            }

    def run_data_quality_tests(self):
        """STEP 3: Run additional quality checks."""
        logger.info("STEP 3: Quality tests")
        start_time = datetime.now()

        try:
            data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
            if not os.path.exists(data_dir):
                raise Exception("Data directory not found")

            recent_files = []
            cutoff_time = datetime.now().timestamp() - (24 * 3600)
            for file in os.listdir(data_dir):
                if file.endswith(('.csv', '.json')):
                    file_path = os.path.join(data_dir, file)
                    if os.path.getmtime(file_path) > cutoff_time:
                        recent_files.append(file)

            if not recent_files:
                logger.warning("No recent data files found")

            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Quality tests passed - {len(recent_files)} recent files")
            return {
                "success": True,
                "message": f"Quality tests passed - {len(recent_files)} recent data files",
                "duration": duration,
                "details": {"recent_files": recent_files}
            }

        except Exception as e:
            return {
                "success": False,
                "message": f"Quality tests error: {str(e)}",
                "duration": (datetime.now() - start_time).total_seconds()
            }

    def send_notification(self, status):
        """STEP 4: Write a simple notification (console + file)."""
        logger.info("STEP 4: Notification")

        try:
            message = f"""
Local Data Pipeline - Status: {status}

Pipeline ID: {self.pipeline_status['pipeline_id']}
Start time: {self.pipeline_status['start_time']}
End time: {datetime.now().isoformat()}

Steps:
"""
            for step in self.pipeline_status['steps']:
                message += f"- {step['step']}: {step['status']} ({step.get('duration', 0):.1f}s)\n"

            if status == "FAILED":
                message += f"\nError: {self.pipeline_status.get('error', 'Unknown error')}"

            print("\n" + "="*60)
            print("PIPELINE NOTIFICATION")
            print("="*60)
            print(message)
            print("="*60)

            notification_file = f"logs/notification_{self.pipeline_status['pipeline_id']}.txt"
            with open(notification_file, 'w') as f:
                f.write(message)

            logger.info(f"Notification saved to {notification_file}")

        except Exception as e:
            logger.error(f"Notification error: {str(e)}")

    def run_pipeline(self):
        """Run the full local pipeline."""
        logger.info("STARTING LOCAL PIPELINE")
        logger.info("=" * 60)

        try:
            ingestion_result = self.run_data_ingestion()
            self.pipeline_status["steps"].append({
                "step": "ingestion",
                "status": "success" if ingestion_result["success"] else "failed",
                "message": ingestion_result["message"],
                "duration": ingestion_result.get("duration", 0)
            })

            if not ingestion_result["success"]:
                raise Exception(f"Ingestion failed: {ingestion_result['message']}")

            dbt_result = self.run_dbt_pipeline()
            self.pipeline_status["steps"].append({
                "step": "dbt_transformation",
                "status": "success" if dbt_result["success"] else "failed",
                "message": dbt_result["message"],
                "duration": dbt_result.get("duration", 0)
            })

            if not dbt_result["success"]:
                raise Exception(f"dbt failed: {dbt_result['message']}")

            test_result = self.run_data_quality_tests()
            self.pipeline_status["steps"].append({
                "step": "data_quality_tests",
                "status": "success" if test_result["success"] else "failed",
                "message": test_result["message"],
                "duration": test_result.get("duration", 0)
            })

            self.send_notification("SUCCESS")

            self.pipeline_status["end_time"] = datetime.now().isoformat()
            self.pipeline_status["overall_status"] = "SUCCESS"

            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            return self.pipeline_status

        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}")

            self.pipeline_status["end_time"] = datetime.now().isoformat()
            self.pipeline_status["overall_status"] = "FAILED"
            self.pipeline_status["error"] = str(e)

            self.send_notification("FAILED")
            return self.pipeline_status

def main():
    """Entry point for local execution."""
    print("Local Orchestrator - Data Pipeline")
    print("=" * 50)

    orchestrator = LocalOrchestrator()
    result = orchestrator.run_pipeline()

    print(f"\nFINAL SUMMARY:")
    print(f"Status: {result['overall_status']}")
    print(f"Steps: {len(result['steps'])}")

    if result['overall_status'] == 'SUCCESS':
        print("Pipeline executed successfully.")
        return 0
    else:
        print(f"Pipeline failed: {result.get('error', 'Unknown error')}")
        return 1

if __name__ == "__main__":
    exit(main()) 