import concurrent.futures


class Executor:

    def __init__(self, executor):
        self.executor = executor
        self.future_by_task = {}
        self.pending_tasks = {}

    def complete_pending(self, task_uid, task_result):
        if task_uid in self.pending_tasks:
            future = self.pending_tasks.pop(task_uid)
            future.set_result(task_result)

    def submit(self, task, task_uid=None):
        self.executor.submit(fn=task.run)
        future = concurrent.futures.Future()
        if task_uid:
            self.pending_tasks[task_uid] = future
        return future

    def schedule(self, task, delay):
        future = self.executor.schedule(fn=task.run, delay=delay)
        self.future_by_task[type(task).__name__] = future

    def cancel(self, task_klass):
        if task_klass.__name__ in self.future_by_task:
            future = self.future_by_task[task_klass.__name__]
            return future.cancel()
        return False
