import logging
import uuid

from apscheduler.events import EVENT_JOB_ERROR
from apscheduler.events import JobExecutionEvent
from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.util import undefined

ROOT_DIR = "/data"

tornado_scheduler = TornadoScheduler()
executors = {"default": TornadoExecutor(max_workers=1)}
tornado_scheduler.configure(executors=executors)

ERROR_LOG_FILE = f"{ROOT_DIR}/aip/logs/apscheduler/{uuid.uuid4()}"
logging.info(f"[ERROR LOG FILE DEFAULT] error log file {ERROR_LOG_FILE}")


def set_error_log_file(file_name):
    global ERROR_LOG_FILE
    file_name = file_name.replace("/", "")
    ERROR_LOG_FILE = f"{ROOT_DIR}/aip/logs/apscheduler/{file_name}"
    logging.warning(f"[ERROR LOG FILE CHANGED] the error log file {ERROR_LOG_FILE} ")


def add_job(job_fn, trigger=None, args=None, kwargs=None, id=None, name=None,
            misfire_grace_time=None, coalesce=undefined, max_instances=3,
            next_run_time=undefined, jobstore='default', executor='default',
            replace_existing=False, **trigger_args):
    tornado_scheduler.add_job(job_fn, trigger=trigger, args=args, kwargs=kwargs, id=id, name=name,
                              misfire_grace_time=misfire_grace_time, coalesce=coalesce, max_instances=max_instances,
                              next_run_time=next_run_time, jobstore=jobstore, executor=executor,
                              replace_existing=replace_existing, **trigger_args)
    tornado_scheduler.add_listener(task_error_callback, EVENT_JOB_ERROR)


def task_error_callback(event: JobExecutionEvent):
    global ERROR_LOG_FILE
    job_id = event.job_id
    exception = event.exception
    line_to_write = f"===========================\n" \
                    f"job_id:{job_id}\n" \
                    f"scheduled_run_time:{event.scheduled_run_time}\n" \
                    f"exception:{exception}\n" \
                    f"{event.traceback}\n" \
                    f"{event.alias}\n" \
                    f"{event}\n" \
                    f"===========================\n"

    logging.error(line_to_write.replace("\n", "  "))
    with open(ERROR_LOG_FILE, "a") as fo:
        fo.write(line_to_write)


def _test():
    add_job(lambda x: print(x), "cron", hour=2, minute=0, second=0)
