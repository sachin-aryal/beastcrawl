#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys


def reset_background_process():
    from app.models import ProcessInfo
    from app.util.helper import kill_process
    for proc in ProcessInfo.objects.all():
        proc.status = "KILLED"
        proc.job_count = 0
        proc.save()
        kill_process(proc.process_id)


def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'bestprices.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)
    reset_background_process()


if __name__ == '__main__':
    main()
