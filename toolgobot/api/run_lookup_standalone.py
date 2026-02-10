
import os
import sys
import json
import logging
import threading
import time
import signal

if len(sys.argv) >= 4:
    _gotax_root = os.path.normpath(os.path.abspath(sys.argv[2]))
    _gobot_root = os.path.normpath(os.path.abspath(sys.argv[3]))
else:
    _script_file = os.path.abspath(__file__)
    _api_dir = os.path.dirname(_script_file)
    _gobot_root = os.path.normpath(os.path.abspath(os.path.dirname(_api_dir)))
    _gotax_root = os.path.normpath(os.path.abspath(os.path.dirname(_gobot_root)))

def _norm_path(p):
    try:
        return os.path.normpath(os.path.abspath(p or "."))
    except Exception:
        return p or ""
_rest = [p for p in sys.path if _norm_path(p) not in (_gotax_root, _gobot_root)]
sys.path[:] = [_gotax_root, _gobot_root] + _rest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_shutdown_requested = False


def main():
    global _shutdown_requested

    if len(sys.argv) < 2:
        logger.error("Usage: python run_lookup_standalone.py <params.json> [GOTAX_ROOT] [GOBOT_ROOT]")
        sys.exit(1)
    params_path = os.path.abspath(sys.argv[1])
    if not os.path.exists(params_path):
        logger.error("Params file not found: %s", params_path)
        sys.exit(1)

    with open(params_path, "r", encoding="utf-8") as f:
        params = json.load(f)

    job_id = params.get("job_id")
    taxcodes = params.get("taxcodes")
    type_taxcode = params.get("type_taxcode")
    id_type = params.get("id_type")
    proxy = params.get("proxy")

    if not job_id or not taxcodes or type_taxcode not in ("cn", "dn"):
        logger.error("Invalid params: job_id, taxcodes (list), type_taxcode (cn|dn) required")
        sys.exit(1)

    _redis_client_path = os.path.join(_gotax_root, "shared", "redis_client.py")
    if not os.path.isfile(_redis_client_path):
        logger.error("shared/redis_client.py not found at %s (GOTAX_ROOT=%s)", _redis_client_path, _gotax_root)
        sys.exit(1)
    try:
        import importlib.util
        _spec = importlib.util.spec_from_file_location("redis_client", _redis_client_path)
        _redis_mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_redis_mod)
        get_redis_client = _redis_mod.get_redis_client
        publish_progress = _redis_mod.publish_progress
    except Exception as e:
        logger.error("Cannot load shared.redis_client: %s", e)
        sys.exit(1)

    try:
        from toolgobot.backend_.backend_service import BackendService
    except ImportError as e:
        logger.error("Cannot import BackendService: %s", e)
        sys.exit(1)

    redis_client = None
    try:
        redis_client = get_redis_client()
    except Exception as e:
        logger.error("Redis connect error: %s", e)
        sys.exit(1)

    def _signal_handler(sig, frame):
        global _shutdown_requested
        if _shutdown_requested:
            return
        _shutdown_requested = True
        logger.info("⏹️ [Job %s] Signal %s received, cancelling...", job_id, sig)
        try:
            redis_client.set(f"job:{job_id}:cancelled", "1")
            redis_client.set(f"job:{job_id}:status", "cancelled")
            publish_progress(job_id, 0, "Yêu cầu đã bị hủy (tool dừng)", data={"type": "error", "error": "Job cancelled"})
        except Exception as e:
            logger.warning("Error setting cancelled flag: %s", e)

    signal.signal(signal.SIGINT, _signal_handler)
    if sys.platform != "win32":
        signal.signal(signal.SIGTERM, _signal_handler)

    def _is_cancelled():
        if _shutdown_requested:
            return True
        try:
            cancelled = redis_client.get(f"job:{job_id}:cancelled")
            if cancelled:
                c = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                if c == '1':
                    return True
            status = redis_client.get(f"job:{job_id}:status")
            if status:
                s = status.decode('utf-8') if isinstance(status, bytes) else str(status).strip()
                if s == 'cancelled':
                    return True
        except Exception:
            pass
        return False

    import time as _time_module
    job_start_time = int(_time_module.time())
    redis_client.set(f"job:{job_id}:status", b"processing")
    redis_client.set(f"job:{job_id}:start_time", str(job_start_time).encode("utf-8"))
    publish_progress(job_id, 0, "Bắt đầu tra cứu...", data={"total": len(taxcodes), "processed": 0})
    logger.info("Standalone script started for job %s, taxcodes=%s", job_id, len(taxcodes))
    sys.stdout.flush()
    sys.stderr.flush()

    is_batch = len(taxcodes) > 1
    result_holder = []
    error_holder = []

    def do_lookup():
        try:
            logger.info("Creating BackendService (proxy=%s)...", "yes" if proxy else "no")
            sys.stdout.flush()
            sys.stderr.flush()
            backend = BackendService(proxy_url=proxy)
            backend._job_id = job_id
            backend._redis_client = redis_client
            logger.info("Calling handle_request...")
            sys.stdout.flush()
            sys.stderr.flush()
            type_data = "1" if type_taxcode == "dn" else "2"
            req = {"type_data": type_data, "raw_data": taxcodes}
            if id_type:
                req["id_type"] = id_type
            result_holder.append(backend.handle_request(req))
        except Exception as e:
            error_holder.append(e)

    if is_batch:
        worker = threading.Thread(target=do_lookup, daemon=True)
        worker.start()
        step = 0
        while worker.is_alive():
            time.sleep(5)
            step += 1
            if _is_cancelled():
                logger.info("[Job %s] Job đã bị cancel, dừng poll", job_id)
                sys.exit(0)
            publish_progress(job_id, min(step * 2, 90), "Đang tra cứu...", data={"total": len(taxcodes), "processed": 0})
        worker.join(timeout=5)
    else:
        do_lookup()

    if _is_cancelled():
        logger.info("[Job %s] Job đã bị cancel sau khi lookup xong", job_id)
        sys.exit(0)

    try:
        if error_holder:
            raise error_holder[0]
        if not result_holder:
            raise RuntimeError("Lookup did not return result")
        result = result_holder[0]
        if isinstance(result, dict) and result.get("status") == "error":
            err_msg = result.get("message", "Unknown error")
            logger.error("Job %s failed (backend): %s", job_id, err_msg)
            redis_client.set(f"job:{job_id}:status", b"failed")
            redis_client.set(f"job:{job_id}:error", err_msg.encode("utf-8"))
            publish_progress(job_id, 0, err_msg, data={"type": "error", "error": err_msg})
            sys.exit(1)
        looked = result.get("looked_info") if isinstance(result, dict) else None
        if isinstance(result, dict) and result.get("status") == "success" and (not looked or len(looked) == 0):
            err_msg = "Khong co du lieu tra cuu (co the loi giai captcha hoac template thieu file)"
            logger.error("Job %s: %s", job_id, err_msg)
            redis_client.set(f"job:{job_id}:status", b"failed")
            redis_client.set(f"job:{job_id}:error", err_msg.encode("utf-8"))
            publish_progress(job_id, 0, err_msg, data={"type": "error", "error": err_msg})
            sys.exit(1)
        result_json = json.dumps(result, ensure_ascii=False)
        redis_client.set(f"job:{job_id}:result", result_json.encode("utf-8"))
        redis_client.set(f"job:{job_id}:status", b"completed")
        publish_progress(job_id, 100, "Hoan thanh", data={"total": len(taxcodes), "processed": len(taxcodes)})
        logger.info("Job %s completed", job_id)
    except Exception as e:
        err_msg = str(e)
        logger.exception("Job %s failed: %s", job_id, err_msg)
        redis_client.set(f"job:{job_id}:status", b"failed")
        redis_client.set(f"job:{job_id}:error", err_msg.encode("utf-8"))
        publish_progress(job_id, 0, f"Lỗi: {err_msg}", data={"type": "error", "error": err_msg})
        sys.exit(1)


if __name__ == "__main__":
    main()
