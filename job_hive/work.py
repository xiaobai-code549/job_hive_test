import time
import traceback
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import wraps
from typing import TYPE_CHECKING, Optional

from job_hive.core import Status
from job_hive.job import Job
from job_hive.logger import LiveLogger
from job_hive.utils import get_now

if TYPE_CHECKING:
    from job_hive.queue import RedisQueue
    from job_hive import Group


class HiveWork:
    def __init__(self, queue: 'RedisQueue'):
        self.logger: Optional[LiveLogger] = None
        self._queue = queue
        self._process_pool: Optional[ProcessPoolExecutor] = None

    def push(self, func, *args, **kwargs) -> str:
        job = Job(func, *args, **kwargs)
        self._queue.enqueue(job)
        return job.job_id

    def pop(self) -> Optional['Job']:
        return self._queue.dequeue()

    def work(self, prefetching: int = 1, waiting: int = 3, concurrent: int = None, result_ttl: int = 24 * 60 * 60):
        # 如果未指定并发数，根据CPU核心数动态分配
        auto_allocated = False
        if concurrent is None:
            concurrent = multiprocessing.cpu_count()
            auto_allocated = True
        
        self.logger = LiveLogger()
        
        if auto_allocated:
            self.logger.info(f"未指定并发数，根据CPU核心数自动分配: {concurrent}")
        
        self.logger.info(r"""
   $$$$$\  $$$$$$\  $$$$$$$\          $$\   $$\ $$$$$$\ $$\    $$\ $$$$$$$$\ 
   \__$$ |$$  __$$\ $$  __$$\         $$ |  $$ |\_$$  _|$$ |   $$ |$$  _____|\n      $$ |$$ /  $$ |$$ |  $$ |        $$ |  $$ |  $$ |  $$ |   $$ |$$ |      
      $$ |$$ |  $$ |$$$$$$$\ |$$$$$$\ $$$$$$$$ |  $$ |  \$$\  $$  |$$$$$\    
$$\   $$ |$$ |  $$ |$$  __$$\ \______|$$  __$$ |  $$ |   \$$\$$  / $$  __|   
$$ |  $$ |$$ |  $$ |$$ |  $$ |        $$ |  $$ |  $$ |    \$$$  /  $$ |      
\$$$$$$  | $$$$$$  |$$$$$$$  |        $$ |  $$ |$$$$$$\    \$  /   $$$$$$$$\ 
 \______/  \______/ \_______/         \__|  __|\______|    \_/    \________|

prefetching: {}
waiting: {}
concurrent: {}
result ttl: {}
Started work...
""".format(prefetching, waiting, concurrent, result_ttl))
        
        try:
            self._process_pool = ProcessPoolExecutor(max_workers=concurrent)
            run_jobs = {}
            while True:
                if len(run_jobs) >= prefetching:
                    flush_jobs = {}
                    for job_id, (future, job) in run_jobs.items():
                        if not future.done():
                            flush_jobs[job_id] = (future, job)
                            continue
                        job.query["ended_at"] = get_now()
                        try:
                            job.query["result"] = str(future.result())
                            job.query["status"] = Status.SUCCESS.value
                            self.logger.info(f"Successes job: {job.job_id}")
                            self._queue.ttl(job.job_id, result_ttl)
                        except Exception as e:
                            job.query["error"] = "{}\n{}".format(e, traceback.format_exc())
                            job.query["status"] = Status.FAILURE.value
                            self.logger.error(f"Failures job: {job.job_id}")
                        finally:
                            self._queue.update_status(job)
                    run_jobs = flush_jobs
                else:
                    job = self.pop()
                    if job is None:
                        time.sleep(waiting)
                        continue
                    self.logger.info(f"Started job: {job.job_id}")
                    future = self._process_pool.submit(job)
                    run_jobs[job.job_id] = (future, job)
                    self._queue.update_status(job)
        except KeyboardInterrupt:
            self.logger.info("工作进程被用户中断")
        except Exception as e:
            self.logger.error(f"工作过程中发生错误: {e}\n{traceback.format_exc()}")
        finally:
            self._cleanup()

    def get_job(self, job_id: str) -> Optional['Job']:
        return self._queue.get_job(job_id)

    def wait(self, job_id: str) -> Optional['Job']:
        while True:
            job: Job = self.get_job(job_id)
            if job is None: return None
            if job.status in (Status.SUCCESS, Status.FAILURE):
                return job
            time.sleep(5)

    def task(self):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs) -> str:
                return self.push(func, *args, **kwargs)

            return wrapper

        return decorator

    def delay_task(self):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs) -> 'Job':
                job = Job(func, *args, **kwargs)
                return job

            return wrapper

        return decorator

    def group_commit(self, group: 'Group'):
        """
        Commit a group of jobs to the queue.
        """
        with group:
            self._queue.enqueue(*group.jobs)
        return group

    def __len__(self) -> int:
        return self._queue.size

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup()

    def __enter__(self):
        return self

    def __del__(self):
        self._cleanup()

    def _cleanup(self):
        """清理资源"""
        # 关闭进程池
        if self._process_pool is not None:
            try:
                self._process_pool.shutdown()
                self._process_pool = None
            except Exception as e:
                if self.logger:
                    self.logger.error(f"关闭进程池时发生错误: {e}")
        # 关闭队列连接池
        try:
            self._queue.close()
        except Exception as e:
            if self.logger:
                self.logger.error(f"关闭队列连接池时发生错误: {e}")

    def __repr__(self):
        return f"<HiveWork queue={self._queue}>"
