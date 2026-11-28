"""Gunicorn hooks for durable background scan recovery."""

def post_worker_init(worker):
    # The database lives on Render's persistent disk. If a worker was restarted
    # during a scan, resume only when a matching unfinished checkpoint exists.
    from app import controller
    controller.resume_pending_scan()

def worker_exit(server, worker):
    # Release the process-owned Longbridge socket before a worker is replaced.
    try:
        from app import controller
        stop_event = getattr(controller, "_daily_history_stop", None)
        if stop_event is not None:
            stop_event.set()
        controller.lb_client.close()
    except Exception:
        worker.log.exception("Failed to stop background workers and close Longbridge context during worker exit")
