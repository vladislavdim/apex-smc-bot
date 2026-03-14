"""
scheduler_manager.py - Управление scheduler с защитой от race conditions
Решает проблемы: одновременные запуски, отсутствие locks, timeout
"""

import asyncio
import threading
import time
import logging
from typing import Dict, Callable, Any, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import functools
from concurrent.futures import ThreadPoolExecutor, Future
import signal
import sys

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

@dataclass
class ScheduledTask:
    name: str
    func: Callable
    interval: int  # в секундах
    timeout: int = 300  # таймаут выполнения в секундах
    enabled: bool = True
    max_concurrent: int = 1  # максимальное количество одновременных запусков
    status: TaskStatus = TaskStatus.PENDING
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    average_duration: float = 0.0
    total_duration: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)
    current_runs: Set[str] = field(default_factory=set)
    created_at: datetime = field(default_factory=datetime.now)

class TaskLock:
    """
    Расширенный lock для задач с отслеживанием владельца
    """
    def __init__(self, name: str):
        self.name = name
        self._lock = threading.RLock()
        self._owner = None
        self._acquired_time = None
        self._acquire_count = 0
    
    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Получение lock с отслеживанием владельца"""
        if self._lock.acquire(timeout=timeout):
            self._owner = threading.current_thread().name
            self._acquired_time = time.time()
            self._acquire_count += 1
            logging.debug(f"Lock {self.name} acquired by {self._owner}")
            return True
        return False
    
    def release(self):
        """Освобождение lock"""
        if self._lock.locked():
            owner = self._owner
            self._owner = None
            self._acquired_time = None
            self._lock.release()
            logging.debug(f"Lock {self.name} released by {owner}")
    
    def is_locked(self) -> bool:
        """Проверка заблокирован ли lock"""
        return self._lock.locked()
    
    def get_owner(self) -> Optional[str]:
        """Получить владельца lock"""
        return self._owner
    
    def get_lock_duration(self) -> float:
        """Получить время удержания lock"""
        if self._acquired_time:
            return time.time() - self._acquired_time
        return 0.0
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

class SchedulerManager:
    def __init__(self, max_workers: int = 10):
        self.tasks: Dict[str, ScheduledTask] = {}
        self.global_lock = threading.RLock()
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="scheduler")
        self.futures: Dict[str, Future] = {}
        self.task_locks: Dict[str, TaskLock] = {}
        self.shutdown_event = threading.Event()
        
        # Статистика
        self.stats = {
            "total_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "timeout_runs": 0,
            "average_queue_time": 0.0,
            "start_time": datetime.now()
        }
        
        # Установка обработчиков сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown"""
        logging.info(f"Received signal {signum}, shutting down scheduler...")
        self.shutdown()
        sys.exit(0)
    
    def add_task(self, name: str, func: Callable, interval: int, 
                 timeout: int = 300, max_concurrent: int = 1, 
                 enabled: bool = True) -> bool:
        """
        Добавление задачи в scheduler
        """
        with self.global_lock:
            if name in self.tasks:
                logging.warning(f"Task {name} already exists")
                return False
            
            task = ScheduledTask(
                name=name,
                func=func,
                interval=interval,
                timeout=timeout,
                max_concurrent=max_concurrent,
                enabled=enabled,
                next_run=datetime.now() + timedelta(seconds=interval)
            )
            
            self.tasks[name] = task
            self.task_locks[name] = TaskLock(name)
            
            logging.info(f"Added task {name} (interval: {interval}s, timeout: {timeout}s)")
            return True
    
    def remove_task(self, name: str) -> bool:
        """
        Удаление задачи из scheduler
        """
        with self.global_lock:
            if name not in self.tasks:
                return False
            
            # Отменяем будущие выполнения
            if name in self.futures:
                self.futures[name].cancel()
                del self.futures[name]
            
            # Удаляем lock
            if name in self.task_locks:
                del self.task_locks[name]
            
            del self.tasks[name]
            logging.info(f"Removed task {name}")
            return True
    
    def enable_task(self, name: str) -> bool:
        """Включение задачи"""
        with self.global_lock:
            if name in self.tasks:
                self.tasks[name].enabled = True
                logging.info(f"Enabled task {name}")
                return True
            return False
    
    def disable_task(self, name: str) -> bool:
        """Отключение задачи"""
        with self.global_lock:
            if name in self.tasks:
                self.tasks[name].enabled = False
                logging.info(f"Disabled task {name}")
                return True
            return False
    
    def _execute_task(self, task: ScheduledTask) -> str:
        """
        Исполнение задачи с защитой от race conditions
        """
        run_id = f"{task.name}_{int(time.time())}"
        start_time = time.time()
        
        try:
            # Проверяем количество одновременных запусков
            with task.lock:
                if len(task.current_runs) >= task.max_concurrent:
                    logging.warning(f"Task {task.name} already running ({len(task.current_runs)} runs)")
                    return None
                    return f"Task {task.name} already running ({len(task.current_runs)} runs)"
                
                task.current_runs.add(run_id)
                task.status = TaskStatus.RUNNING
                task.last_run = datetime.now()
            
            logging.info(f"Starting task {task.name} (run_id: {run_id})")
            
            # Выполнение задачи с таймаутом
            if asyncio.iscoroutinefunction(task.func):
                # Асинхронная функция
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(
                        asyncio.wait_for(task.func(), timeout=task.timeout)
                    )
                finally:
                    loop.close()
            else:
                # Синхронная функция
                result = task.func()
            
            # Успешное завершение
            duration = time.time() - start_time
            with task.lock:
                task.current_runs.discard(run_id)
                task.status = TaskStatus.COMPLETED
                task.run_count += 1
                task.total_duration += duration
                task.average_duration = task.total_duration / task.run_count
                task.next_run = datetime.now() + timedelta(seconds=task.interval)
            
            # Обновление статистики
            with self.global_lock:
                self.stats["total_runs"] += 1
                self.stats["successful_runs"] += 1
            
            logging.info(f"Task {task.name} completed in {duration:.2f}s")
            return f"Task {task.name} completed successfully"
            
        except asyncio.TimeoutError:
            duration = time.time() - start_time
            with task.lock:
                task.current_runs.discard(run_id)
                task.status = TaskStatus.TIMEOUT
                task.error_count += 1
                task.last_error = f"Timeout after {duration:.2f}s"
                task.next_run = datetime.now() + timedelta(seconds=task.interval)
            
            with self.global_lock:
                self.stats["total_runs"] += 1
                self.stats["timeout_runs"] += 1
            
            logging.error(f"Task {task.name} timed out after {duration:.2f}s")
            return f"Task {task.name} timed out"
            
        except Exception as e:
            duration = time.time() - start_time
            with task.lock:
                task.current_runs.discard(run_id)
                task.status = TaskStatus.FAILED
                task.error_count += 1
                task.last_error = str(e)
                task.next_run = datetime.now() + timedelta(seconds=task.interval)
            
            with self.global_lock:
                self.stats["total_runs"] += 1
                self.stats["failed_runs"] += 1
            
            logging.error(f"Task {task.name} failed: {e}")
            return f"Task {task.name} failed: {e}"
    
    def _schedule_task(self, task: ScheduledTask):
        """
        Планирование выполнения задачи
        """
        if not task.enabled or self.shutdown_event.is_set():
            return
        
        now = datetime.now()
        
        # Проверяем время следующего запуска
        if task.next_run and now < task.next_run:
            return
        
        # Проверяем не запущена ли уже задача
        task_lock = self.task_locks.get(task.name)
        if task_lock and task_lock.is_locked():
            # Проверяем как долго удерживается lock
            lock_duration = task_lock.get_lock_duration()
            if lock_duration > task.timeout:
                logging.warning(f"Task {task.name} lock held for {lock_duration:.2f}s, forcing release")
                task_lock.release()
            else:
                return  # Задача уже выполняется
        
        # Планируем выполнение
        try:
            future = self.executor.submit(self._execute_task, task)
            self.futures[task.name] = future
            
            # Добавляем callback для очистки
            future.add_done_callback(lambda f: self._task_completed(task.name, f))
            
        except Exception as e:
            logging.error(f"Failed to schedule task {task.name}: {e}")
    
    def _task_completed(self, task_name: str, future: Future):
        """
        Callback завершения задачи
        """
        try:
            result = future.result()
            logging.debug(f"Task {task_name} result: {result}")
        except Exception as e:
            logging.error(f"Task {task_name} exception: {e}")
        finally:
            # Очищаем future
            if task_name in self.futures:
                del self.futures[task_name]
    
    def start(self):
        """
        Запуск scheduler
        """
        with self.global_lock:
            if self.running:
                logging.warning("Scheduler is already running")
                return
            
            self.running = True
            self.shutdown_event.clear()
            
            # Запускаем основной цикл в отдельном потоке
            self.scheduler_thread = threading.Thread(
                target=self._scheduler_loop,
                name="scheduler_main",
                daemon=True
            )
            self.scheduler_thread.start()
            
            logging.info("Scheduler started")
    
    def stop(self):
        """
        Остановка scheduler
        """
        with self.global_lock:
            if not self.running:
                return
            
            self.running = False
            self.shutdown_event.set()
            
            # Отменяем все будущие задачи
            for future in self.futures.values():
                future.cancel()
            self.futures.clear()
            
            # Ждём завершения основного потока
            if hasattr(self, 'scheduler_thread'):
                self.scheduler_thread.join(timeout=5)
            
            logging.info("Scheduler stopped")
    
    def shutdown(self):
        """
        Graceful shutdown scheduler
        """
        logging.info("Shutting down scheduler...")
        self.stop()
        self.executor.shutdown(wait=True)
        logging.info("Scheduler shutdown complete")
    
    def _scheduler_loop(self):
        """
        Основной цикл scheduler
        """
        logging.info("Scheduler loop started")
        
        while self.running and not self.shutdown_event.is_set():
            try:
                now = datetime.now()
                
                # Планируем задачи
                with self.global_lock:
                    tasks_to_schedule = [
                        task for task in self.tasks.values()
                        if task.enabled and (
                            not task.next_run or now >= task.next_run
                        )
                    ]
                
                for task in tasks_to_schedule:
                    self._schedule_task(task)
                
                # Небольшая задержка чтобы не нагружать CPU
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"Scheduler loop error: {e}")
                time.sleep(5)
        
        logging.info("Scheduler loop ended")
    
    def get_task_status(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Получение статуса задачи
        """
        with self.global_lock:
            if name not in self.tasks:
                return None
            
            task = self.tasks[name]
            task_lock = self.task_locks.get(name)
            
            return {
                "name": task.name,
                "status": task.status.value,
                "enabled": task.enabled,
                "last_run": task.last_run.isoformat() if task.last_run else None,
                "next_run": task.next_run.isoformat() if task.next_run else None,
                "run_count": task.run_count,
                "error_count": task.error_count,
                "last_error": task.last_error,
                "average_duration": task.average_duration,
                "current_runs": len(task.current_runs),
                "is_locked": task_lock.is_locked() if task_lock else False,
                "lock_owner": task_lock.get_owner() if task_lock else None,
                "lock_duration": task_lock.get_lock_duration() if task_lock else 0.0
            }
    
    def get_all_tasks_status(self) -> Dict[str, Any]:
        """
        Получение статуса всех задач
        """
        with self.global_lock:
            return {
                "scheduler_running": self.running,
                "total_tasks": len(self.tasks),
                "enabled_tasks": sum(1 for t in self.tasks.values() if t.enabled),
                "running_tasks": sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING),
                "stats": self.stats.copy(),
                "tasks": {
                    name: self.get_task_status(name)
                    for name in self.tasks.keys()
                }
            }
    
    def force_run_task(self, name: str) -> bool:
        """
        Принудительный запуск задачи
        """
        with self.global_lock:
            if name not in self.tasks:
                return False
            
            task = self.tasks[name]
            if not task.enabled:
                return False
            
            # Сбрасываем время следующего запуска
            task.next_run = datetime.now()
            
            logging.info(f"Force running task {name}")
            return True
    
    def reset_task_errors(self, name: str) -> bool:
        """
        Сброс счётчика ошибок задачи
        """
        with self.global_lock:
            if name not in self.tasks:
                return False
            
            task = self.tasks[name]
            with task.lock:
                task.error_count = 0
                task.last_error = None
            
            logging.info(f"Reset errors for task {name}")
            return True

# Глобальный экземпляр scheduler
_scheduler = SchedulerManager()

def get_scheduler() -> SchedulerManager:
    """Получить глобальный экземпляр scheduler"""
    return _scheduler

# Декоратор для автоматического добавления задач в scheduler
def scheduled_task(name: str, interval: int, timeout: int = 300, 
                  max_concurrent: int = 1, enabled: bool = True):
    """
    Декоратор для автоматического добавления функции в scheduler
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        
        # Добавляем задачу в scheduler
        _scheduler.add_task(
            name=name,
            func=wrapper,
            interval=interval,
            timeout=timeout,
            max_concurrent=max_concurrent,
            enabled=enabled
        )
        
        return wrapper
    return decorator

# Удобные функции для управления
def start_scheduler():
    """Запуск scheduler"""
    _scheduler.start()

def stop_scheduler():
    """Остановка scheduler"""
    _scheduler.stop()

def shutdown_scheduler():
    """Graceful shutdown scheduler"""
    _scheduler.shutdown()

def add_scheduled_task(name: str, func: Callable, interval: int, **kwargs):
    """Добавление задачи в scheduler"""
    return _scheduler.add_task(name, func, interval, **kwargs)

def get_task_info(name: str) -> Optional[Dict[str, Any]]:
    """Получение информации о задаче"""
    return _scheduler.get_task_status(name)

def get_scheduler_info() -> Dict[str, Any]:
    """Получение информации о scheduler"""
    return _scheduler.get_all_tasks_status()
