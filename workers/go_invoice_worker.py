#!/usr/bin/env python3
"""
Go-Invoice Worker (Template)
Consume jobs from Redis queue and process
"""
import sys
import os
import json
import logging

# Get the project root directory (tool-gotax)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import shared modules
from shared.redis_client import get_redis_client, publish_progress

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis queues
QUEUE_GO_INVOICE = 'go-invoice:jobs'

def process_go_invoice_job(job_data):
    """Process go-invoice job"""
    job_id = job_data.get('job_id')
    params = job_data.get('params', {})
    
    redis_client = get_redis_client()
    
    try:
        # Update status: processing
        redis_client.set(f"job:{job_id}:status", "processing")
        
        # Publish initial progress
        publish_progress(job_id, 0, "Bắt đầu xử lý...")
        
        # TODO: Implement go-invoice logic
        # Import go-invoice services và xử lý
        
        # For now, just simulate
        publish_progress(job_id, 50, "Đang xử lý...")
        # ... processing logic ...
        publish_progress(job_id, 100, "Hoàn thành!")
        
        # Save result
        result_data = {
            'status': 'success',
            'message': 'Processed successfully',
        }
        
        redis_client.set(f"job:{job_id}:result", json.dumps(result_data))
        redis_client.set(f"job:{job_id}:status", "completed")
        
    except Exception as e:
        logger.error(f"Error processing job {job_id}: {e}")
        redis_client.set(f"job:{job_id}:status", "failed")
        redis_client.set(f"job:{job_id}:error", str(e))
        publish_progress(job_id, 0, f"Lỗi: {str(e)}")

def main():
    """Main worker loop"""
    redis_client = get_redis_client()
    logger.info(f"Go-Invoice Worker started. Listening on queue: {QUEUE_GO_INVOICE}")
    
    while True:
        try:
            # Blocking pop from queue (wait up to 1 second)
            result = redis_client.blpop([QUEUE_GO_INVOICE], timeout=1)
            
            if result:
                queue_name, job_data_json = result
                job_data = json.loads(job_data_json)
                
                logger.info(f"Processing job: {job_data.get('job_id')}")
                
                # Process job
                process_go_invoice_job(job_data)
                
        except KeyboardInterrupt:
            logger.info("Worker stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in worker loop: {e}")
            import time
            time.sleep(5)  # Wait before retry

if __name__ == '__main__':
    main()

