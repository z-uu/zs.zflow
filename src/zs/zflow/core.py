import logging
import os
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from zs.zflow.builtin import BUILTIN_METHODS
from zs.zflow.utils import gather_zuto_mods, import_file
from ._internal import ZFlowTask, ZFlowQueue, STEPTYPE
from ._misc import fileLogger
import time
import typing
import traceback
from zuu.util_procLifetime import cleanup
import subprocess
import sys
from .utils import lifetime, timed_input

class ZFlow:
    def __init__(self, folder : str):
        assert os.path.exists(folder), f"Folder {folder} does not exist"
        folder = os.path.abspath(folder)
        assert os.path.isdir(folder), f"Folder {folder} is not a directory"
        self.path = folder
        self.queue = ZFlowQueue()
        self.queue.from_folder(folder)

        self.funcMaps : typing.Dict[str, typing.Callable] = BUILTIN_METHODS.copy()

        self.currentTaskThread = None
        self.runHistory = []

        self.observer = Observer()
        self.observer.schedule(FileHandler(self), path=folder, recursive=True)

        self.disabled_tasks = []
        self._load_state()

    def _setup_funcmaps(self):
        
        # Load standard library modules
        self.funcMaps.update(
            gather_zuto_mods(os.path.dirname(os.path.abspath(__file__)))
        )
        self.funcMaps.update(
            gather_zuto_mods(
                os.path.join(
                    os.path.dirname(os.path.dirname(sys.executable)),
                    "Lib",
                    "site-packages",
                    "zs",
                    "zuto",
                )
            )
        )

        # Load user-defined .py files in workflow directory
        for file in os.listdir(self.path):
            if file.endswith(".py"):
                try:
                    mod = import_file(os.path.join(self.path, file), "zs.zuto.funcs")
                    eligibles = [name for name in dir(mod.Cmds) if not name.startswith("_")]
                    for name in eligibles:
                        if name in self.funcMaps:
                            print(f"Warning: Function {name} already exists in funcMaps")
                            continue
                        self.funcMaps[name] = getattr(mod.Cmds, name)
                except Exception as e:
                    print(f"Error processing {file}: {e}")

    def _handle_func(self, params : dict):
        key, x = params.popitem()
        if key not in self.funcMaps:
            raise ValueError(f"Function {key} not found")
        return self.funcMaps[key](x=x, **params)

    def _handle_cmd(self, string : str):
        print(f"Step: {string[:40]}...")
        res = subprocess.Popen(
            string,
            shell=True,
        )
        res.wait()
        if res.returncode != 0:
            print(f"Step {string} failed with return code {res.returncode}")
            return

    def _handle_steps(self, steps : typing.List[STEPTYPE]):
        for step in steps:
            if isinstance(step, dict):
                self._handle_func(step.copy())
            elif isinstance(step, str):
                self._handle_cmd(step)
            else:
                raise ValueError(f"Invalid step type: {type(step)}")
            time.sleep(0.3)


    def _execute_thread(self, task : ZFlowTask):
        try:
            print(f"======Task started {task.name}======\n")
            if task.description:
                print(task.description)

            if task.init and task.id not in self.runHistory:
                self._handle_steps(task.init)

            try:
                self._handle_steps(task.steps)
            except Exception as e:
                logging.error(f"task {task.name} ({task.id}) error type {type(e)}")
                fileLogger.error(f"identifier: {task.id}")
                fileLogger.error(f"name: {task.name}")
                fileLogger.error("prior on error handling")
                fileLogger.error(f"error: {e}")
                fileLogger.error(f"traceback: {traceback.format_exc()}")
                if task.onError:
                    logging.error(f"running onError steps for task {task.name}")
                    self._handle_steps(task.onError)
                else:
                    raise e
                
        except Exception as e:
            logging.error(f"task {task.name} ({task.id}) error type {type(e)}, task aborted")
            fileLogger.error(f"identifier: {task.id}")
            fileLogger.error(f"name: {task.name}")
            fileLogger.error(f"error: {e}")
            fileLogger.error(f"traceback: {traceback.format_exc()}")
        finally:
            self.runHistory.append(task.id)
            print(f"======Task {task.name} finished======\n")
            self.currentTaskThread = None
            self.currentTask = None
        
    def _run(self):
        
        while True:
            if not self.currentTaskThread and (ttuple := self.queue.peek()) is not None:
                task = ttuple[2]
                if task.canRun:
                    self.queue.dequeue()
                    
                    if task.name in self.disabled_tasks or task.filename in self.disabled_tasks:
                        print(f"Task {task.name} is disabled, skipping")
                        continue

                    func = self._execute_thread
                    
                    
                    if task.lifetime:
                        func = lifetime(task.lifetime)(func)

                    if task.cleanup:
                        func = cleanup(True, True, True)(func)
                    
                    self.currentTaskThread = threading.Thread(target=func, args=(task,))
                    self.currentTask = task
                    self.currentTaskThread.start()
            time.sleep(1)

    def run(self):
        try:
            self._setup_funcmaps()
            self.observer.start()
            self._run()
        except KeyboardInterrupt:
            skip = timed_input("If you want to just skip this task, enter 'y' else enter/wait", 5)
            if skip == "y":
                if self.currentTaskThread:
                    import ctypes
                    ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(self.currentTaskThread.ident), ctypes.py_object(SystemExit))
                    self.currentTaskThread = None
                    print(f"Task skipped for {self.currentTask.name}")
                    self.currentTask = None
                else:
                    obj = self.queue.dequeue()
                    if obj:
                        print(f"Task skipped for {obj[2].name}")
                self._run()

            self.observer.stop()
            self.observer.join()
            import ctypes
            # use ctypes to kill all threads
            ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(threading.get_ident()), ctypes.py_object(SystemExit))
            print("Observer stopped")
                
    def _load_state(self):
        state_path = os.path.join(self.path, 'state.toml')
        if not os.path.exists(state_path):
            return
        try:
            import toml
            with open(state_path, 'r') as f:
                state = toml.load(f)
            self.disabled_tasks = state.get('disabled', [])
        except ImportError:
            logging.error("toml module not installed, cannot load state.toml")
        except Exception as e:
            logging.error(f"Error loading state.toml: {e}")

class FileHandler(FileSystemEventHandler):
    def __init__(self, scheduler: ZFlow):
        super().__init__()
        self.scheduler = scheduler
        self._last_event_time = 0
        self._pending_events = {}

    def _debounced_processing(self, path: str, processor: callable):
        def wrapper():
            current_time = time.monotonic()
            if current_time - self._last_event_time >= 2:
                self._last_event_time = current_time
                processor()
                # Clear pending events after successful processing
                self._pending_events.pop(path, None)
            else:
                # Reschedule check if we have newer events
                if path in self._pending_events:
                    threading.Timer(2, self._debounced_processing(path, processor)).start()

        # Cancel any existing timer for this path
        if path in self._pending_events:
            self._pending_events.pop(path).cancel()
        
        # Store the timer so we can cancel it if needed
        timer = threading.Timer(2, wrapper)
        self._pending_events[path] = timer
        timer.start()

    def on_modified(self, event):
        if event.is_directory or not event.src_path.startswith(self.scheduler.path):
            return

        filename = os.path.basename(event.src_path)
        path = event.src_path

        if filename.endswith(('.yml', '.yaml')):
            self._debounced_processing(path, lambda: self._handle_yml(path))
        elif filename == 'state.toml':
            self._debounced_processing(path, lambda: self.scheduler._load_state())

    def _handle_yml(self, path : str):
        print(f"Detected change in {path}")
        self.scheduler.queue.from_file(path)
