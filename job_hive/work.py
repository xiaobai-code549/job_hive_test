import time
import traceback
import os
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

    def work(self, prefetching: int = 0, waiting: int = 3, concurrent: int = 0, result_ttl: int = 24 * 60 * 60):
        # 如果concurrent为0，则根据CPU核心数动态分配
        if concurrent <= 0:
            concurrent = multiprocessing.cpu_count()
        
        # 如果prefetching为0，则设置为concurrent的2倍
        if prefetching <= 0:
            prefetching = concurrent * 2
        
        self.logger = LiveLogger()
        self.logger.info(r"""
   $$$$$\  $$$$$$\  $$$$$$$\          $$\   $$\ $$$$$$\ $$\    $$\ $$$$$$$$\ 
   \__$$ |$$  __$$\ $$  __$$\         $$ |  $$ |\_$$  _|$$ |   $$ |$$  _____| 
      $$ |$$ /  $$ |$$ |  $$ |        $$ |  $$ |  $$ |  $$ |   $$ |$$ |       
      $$ |$$ |  $$ |$$$$$$$\ |$$$$$$\ $$$$$$$$ |  $$ |  \$$\  $$  |$$$$$\     
$$\   $$ |$$ |  $$ |$$  __$$\ \______|$$  __$$ |  $$ |   \$$\$$  / $$  __|    
$$ |  $$ |$$ |  $$ |$$ |  $$ |        $$ |  $$ |  $$ |    \$$$  /  $$ |      
\$$$$$$  | $$$$$$  |$$$$$$$  |        $$ |  $$ |$$$$$$\    \$  /   $$$$$$$$\ 
 \______/  \______/ \_______/         \__|  \__|\______|    \_/    \________| 

prefetching: {}
waiting: {}
concurrent: {}
result ttl: {}
Started work...
""".format(prefetching, waiting, concurrent, result_ttl))
        
        # 记录任务执行时间统计
        job_stats = {
            'total': 0,
            'success': 0,
            'failure': 0,
            'total_time': 0
        }
        
        self._process_pool = ProcessPoolExecutor(max_workers=concurrent)
        run_jobs = {}
        
        try:
            while True:
                if len(run_jobs) >= prefetching:
                    flush_jobs = {}
                    for job_id, (future, job, start_time) in run_jobs.items():
                        if not future.done():
                            flush_jobs[job_id] = (future, job, start_time)
                            continue
                        
                        # 计算任务执行时间
                        execution_time = time.time() - start_time
                        job_stats['total'] += 1
                        job_stats['total_time'] += execution_time
                        
                        job.query["ended_at"] = get_now()
                        job.query["execution_time"] = execution_time
                        
                        try:
                            job.query["result"] = str(future.result())
                            job.query["status"] = Status.SUCCESS.value
                            job_stats['success'] += 1
                            self.logger.info(f"Successes job: {job.job_id} (took {execution_time:.2f}s)")
                            self._queue.ttl(job.job_id, result_ttl)
                        except Exception as e:
                            job.query["error"] = "{}\n{}".format(e, traceback.format_exc())
                            job.query["status"] = Status.FAILURE.value
                            job_stats['failure'] += 1
                            self.logger.error(f"Failures job: {job.job_id} (took {execution_time:.2f}s)")
                        finally:
                            self._queue.update_status(job)
                    run_jobs = flush_jobs
                    
                    # 每处理10个任务打印一次统计信息
                    if job_stats['total'] > 0 and job_stats['total'] % 10 == 0:
                        avg_time = job_stats['total_time'] / job_stats['total'] if job_stats['total'] > 0 else 0
                        success_rate = (job_stats['success'] / job_stats['total']) * 100 if job_stats['total'] > 0 else 0
                        self.logger.info(f"Job stats: Total={job_stats['total']}, Success={job_stats['success']}, "
                                     f"Failure={job_stats['failure']}, Avg time={avg_time:.2f}s, "
                                     f"Success rate={success_rate:.1f}%")
                else:
                    job = self.pop()
                    if job is None:
                        time.sleep(waiting)
                        continue
                    self.logger.info(f"Started job: {job.job_id}")
                    start_time = time.time()
                    future = self._process_pool.submit(job)
                    run_jobs[job.job_id] = (future, job, start_time)
                    self._queue.update_status(job)
        finally:
            # 关闭进程池
            if self._process_pool:
                self._process_pool.shutdown()
                self.logger.info("Process pool shutdown completed")
                
                # 打印最终统计信息
                if job_stats['total'] > 0:
                    avg_time = job_stats['total_time'] / job_stats['total']
                    success_rate = (job_stats['success'] / job_stats['total']) * 100
                    self.logger.info(f"Final job stats: Total={job_stats['total']}, Success={job_stats['success']}, "
                                 f"Failure={job_stats['failure']}, Avg time={avg_time:.2f}s, "
                                 f"Success rate={success_rate:.1f}%")
            
            # 关闭队列连接
            if hasattr(self._queue, 'close'):
                self._queue.close()
                if self.logger:
                    self.logger.info("Queue connection closed")

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
        # 关闭进程池
        if self._process_pool is not None:
            self._process_pool.shutdown()
        # 关闭队列连接
        if hasattr(self._queue, 'close'):
            self._queue.close()

    def __enter__(self):
        return self

    def __del__(self):
        # 关闭进程池
        if self._process_pool is not None:
            self._process_pool.shutdown()
        # 关闭队列连接
        if hasattr(self._queue, 'close'):
            self._queue.close()

    def __repr__(self):
        return f"<HiveWork queue={self._queue}>"
