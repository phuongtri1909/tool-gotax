#!/usr/bin/env python3
"""
Go-Bot Worker
Consume jobs from Redis queue, call API server /api/go-bot/lookup/queue,
API server chạy lookup trong background và ghi progress/result vào Redis.
Worker chỉ gọi API rồi poll Redis chờ hoàn thành (giống Go Invoice).
"""
import sys
import os
import json
import asyncio
import logging
import httpx

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from shared.redis_client import get_redis_client, publish_progress

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

QUEUE_GO_BOT = 'go-bot:jobs'
API_SERVER_URL = os.getenv('GO_BOT_API_URL', 'http://127.0.0.1:5000/api/go-bot')


async def process_go_bot_job(job_data):
    """
    Gửi job tới API /lookup/queue, API chạy lookup trong background và ghi Redis.
    Worker poll Redis cho tới khi completed/failed/cancelled.
    """
    job_id = job_data.get('job_id')
    params = job_data.get('params', {})

    redis_client = get_redis_client()

    try:
        import time as _time_module
        job_start_time = int(_time_module.time())
        redis_client.set(f"job:{job_id}:status", "processing".encode('utf-8'))
        redis_client.set(f"job:{job_id}:start_time", str(job_start_time).encode('utf-8'))

        taxcodes = params.get('taxcodes')
        type_taxcode = params.get('type_taxcode')
        id_type = params.get('id_type')
        proxy = params.get('proxy')

        if not taxcodes or not isinstance(taxcodes, list) or len(taxcodes) == 0:
            error_msg = "Missing or invalid 'taxcodes' (non-empty list required)"
            logger.error(f"[Job {job_id}] {error_msg}")
            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
            publish_progress(job_id, 0, error_msg)
            return

        if type_taxcode not in ['cn', 'dn']:
            error_msg = "'type_taxcode' must be 'cn' or 'dn'"
            logger.error(f"[Job {job_id}] {error_msg}")
            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
            publish_progress(job_id, 0, error_msg)
            return

        request_data = {
            'job_id': job_id,
            'taxcodes': taxcodes,
            'type_taxcode': type_taxcode,
        }
        if id_type:
            request_data['id_type'] = id_type
        if proxy:
            request_data['proxy'] = proxy

        logger.info(f"[Job {job_id}] Gọi API: {API_SERVER_URL}/lookup/queue")
        publish_progress(job_id, 0, "Bắt đầu tra cứu...")

        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.post(
                    f"{API_SERVER_URL}/lookup/queue",
                    json=request_data,
                    timeout=10.0
                )
            except httpx.TimeoutException:
                error_msg = "Timeout khi gọi API server"
                logger.error(f"[Job {job_id}] {error_msg}")
                redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                publish_progress(job_id, 0, error_msg)
                return
            except Exception as e:
                error_msg = f"Lỗi kết nối API: {e}"
                logger.error(f"[Job {job_id}] {error_msg}")
                redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                publish_progress(job_id, 0, error_msg)
                return

            if response.status_code != 202:
                try:
                    err_data = response.json()
                    error_msg = err_data.get('message', f"API trả về {response.status_code}")
                except Exception:
                    error_msg = f"API trả về {response.status_code}"
                logger.error(f"[Job {job_id}] {error_msg}")
                redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                publish_progress(job_id, 0, error_msg)
                return

            resp_data = response.json()
            if resp_data.get('status') != 'accepted':
                error_msg = resp_data.get('message', 'API không chấp nhận request')
                logger.error(f"[Job {job_id}] {error_msg}")
                redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                publish_progress(job_id, 0, error_msg)
                return

        logger.info(f"[Job {job_id}] API đã chấp nhận, đang đợi Redis completed...")

        max_wait_time = 7200
        poll_interval = 2
        waited_time = 0
        last_log_time = 0

        while waited_time < max_wait_time:
            await asyncio.sleep(poll_interval)
            waited_time += poll_interval

            if waited_time - last_log_time >= 10:
                logger.info(f"[Job {job_id}] Đang xử lý... (đã đợi {waited_time}s)")
                last_log_time = waited_time

            cancelled = redis_client.get(f"job:{job_id}:cancelled")
            if cancelled:
                c = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                if c == '1':
                    logger.info(f"[Job {job_id}] Job đã bị cancel")
                    redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                    publish_progress(job_id, 0, "Yêu cầu đã bị hủy")
                    return

            status = redis_client.get(f"job:{job_id}:status")
            if status:
                status = status.decode('utf-8') if isinstance(status, bytes) else str(status).strip()
                if status == 'completed':
                    logger.info(f"[Job {job_id}] Job hoàn thành")
                    return
                if status == 'failed':
                    err = redis_client.get(f"job:{job_id}:error")
                    if err:
                        err = err.decode('utf-8') if isinstance(err, bytes) else str(err)
                    logger.error(f"[Job {job_id}] Job failed: {err}")
                    return
                if status == 'cancelled':
                    logger.info(f"[Job {job_id}] Job đã bị cancel")
                    return

        logger.warning(f"[Job {job_id}] Timeout sau {max_wait_time}s")
        redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
        redis_client.set(f"job:{job_id}:error", "Timeout chờ kết quả".encode('utf-8'))
        publish_progress(job_id, 0, "Timeout chờ kết quả")

    except asyncio.CancelledError:
        logger.info(f"[Job {job_id}] Task bị cancel (Ctrl+C)")
        try:
            redis_client.set(f"job:{job_id}:cancelled", "1".encode('utf-8'))
            redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
            publish_progress(job_id, 0, "Yêu cầu đã bị hủy")
        except Exception:
            pass
        return

    except Exception as e:
        error_msg = str(e)
        logger.error(f"[Job {job_id}] Exception: {error_msg}", exc_info=True)
        redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
        redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
        publish_progress(job_id, 0, f"Lỗi: {error_msg}")


async def worker_loop(redis_client, semaphore, max_concurrent=3):
    active_tasks = set()
    while True:
        try:
            completed = [t for t in active_tasks if t.done()]
            for t in completed:
                active_tasks.discard(t)
                try:
                    t.result()
                except Exception as e:
                    logger.error(f"Task error: {e}", exc_info=True)

            if len(active_tasks) < max_concurrent:
                result = await asyncio.to_thread(redis_client.blpop, [QUEUE_GO_BOT], timeout=1)
                if result:
                    queue_name, job_data_json = result
                    if isinstance(job_data_json, bytes):
                        job_data_json = job_data_json.decode('utf-8')
                    try:
                        job_data = json.loads(job_data_json)
                        job_id = job_data.get('job_id')
                        logger.info(f"Received job: {job_id}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Parse job error: {e}")
                        continue
                    async def run_job():
                        async with semaphore:
                            await process_go_bot_job(job_data)
                    task = asyncio.create_task(run_job())
                    active_tasks.add(task)
                else:
                    await asyncio.sleep(0.5)
            else:
                await asyncio.sleep(0.5)
        except KeyboardInterrupt:
            logger.info("⏹️ Worker dừng bởi người dùng (Ctrl+C)")

            try:
                keys = redis_client.keys("job:*:status")
                for key in keys:
                    if isinstance(key, bytes):
                        key = key.decode('utf-8')
                    status = redis_client.get(key)
                    if status:
                        status = status.decode('utf-8') if isinstance(status, bytes) else str(status).strip()
                        if status == 'processing':
                            job_id = key.split(':')[1]
                            logger.info(f"⏹️ Setting cancelled flag for job {job_id}")
                            redis_client.set(f"job:{job_id}:cancelled", "1")
                            redis_client.set(f"job:{job_id}:status", "cancelled")
                            publish_progress(job_id, 0, "Yêu cầu đã bị hủy (worker dừng)")
            except Exception as e:
                logger.warning(f"Error setting cancelled flags: {e}")

            if active_tasks:
                logger.info(f"⏳ Đang hủy {len(active_tasks)} task đang chạy...")
                for t in active_tasks:
                    if not t.done():
                        t.cancel()
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*active_tasks, return_exceptions=True),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    logger.info("⏹️ Timeout khi đợi tasks, đang force cancel...")
            break
        except Exception as e:
            logger.error(f"Worker loop error: {e}", exc_info=True)
            await asyncio.sleep(5)


def main():
    redis_client = get_redis_client()
    try:
        redis_client.ping()
    except Exception as e:
        logger.error("Lỗi Redis: %s" % e)
        return
    max_concurrent = int(os.getenv('WORKER_MAX_CONCURRENT', '3'))
    logger.info("Go-Bot Worker ready | queue: %s, max: %s" % (QUEUE_GO_BOT, max_concurrent))

    semaphore = asyncio.Semaphore(max_concurrent)
    try:
        asyncio.run(worker_loop(redis_client, semaphore, max_concurrent))
    except KeyboardInterrupt:
        logger.info("Worker đã dừng")


if __name__ == '__main__':
    main()
