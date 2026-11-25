"""Gunicorn hooks for durable background scan recovery."""

def post_worker_init(worker):
    # The database lives on Render's persistent disk. If a worker was restarted
    # during a scan, resume only when a matching unfinished checkpoint exists.
    from app import controller
    controller.resume_pending_scan()
