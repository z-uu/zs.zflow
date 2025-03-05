import ctypes
import datetime
import os
import threading
from functools import wraps
from zuu.util_timeparse import time_parse
import sys
import importlib
from zuu.stdext_importlib import import_file #noqa

def lifetime(
    timestr: str,
):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get target termination time and ensure it's a timestamp
            target_time: datetime.datetime = time_parse(timestr)
            print(target_time)
            result = [None]
            exception = [None]
            thread_id = None

            def async_raise(target_tid, exc_type):
                """Raises an exception in the target thread"""
                res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_long(target_tid), ctypes.py_object(exc_type)
                )
                if res == 0:
                    raise ValueError("Invalid thread ID")
                elif res != 1:
                    ctypes.pythonapi.PyThreadState_SetAsyncExc(target_tid, None)
                    raise SystemError("Failed to set exception")

            def run_func():
                nonlocal thread_id
                thread_id = threading.get_ident()
                try:
                    result[0] = func(*args, **kwargs)
                except Exception as e:
                    exception[0] = e

            thread = threading.Thread(target=run_func)
            thread.daemon = True
            thread.start()

            wait_time = max((target_time - datetime.datetime.now()).total_seconds(), 0)
            thread.join(wait_time)

            if thread.is_alive():
                try:
                    print(f"Lifetime expired, terminating thread {thread_id}")
                    async_raise(thread_id, SystemExit)
                    thread.join(0.1)  # Give it a moment to clean up
                except Exception:
                    pass
                return None

            if exception[0] is not None:
                raise exception[0]

            return result[0]

        return wrapper

    return decorator


realPath = os.path.dirname(os.path.abspath(__file__))


def gather_zuto_mods(dirpath: str):
    funcs = {}
    if not os.path.exists(dirpath):
        return funcs

    # Add the directory to sys.path temporarily
    sys.path.insert(0, dirpath)
    try:
        for path in os.listdir(dirpath):
            fullpath = os.path.join(dirpath, path)
            if os.path.isdir(fullpath):
                if path.startswith("_"):
                    continue

                # Check for valid Python package
                init_py = os.path.join(fullpath, "__init__.py")
                if not os.path.exists(init_py):
                    continue

                try:
                    # Use proper module naming convention
                    mod_name = f"zs.zflow.{path}" if dirpath == realPath else path
                    mod = importlib.import_module(mod_name)

                    if not hasattr(mod, "Cmds"):
                        print(
                            f"Warning: Module {mod_name} has no Cmds class",
                            file=sys.stderr,
                        )
                        continue

                    # Collect commands
                    for name in dir(mod.Cmds):
                        if name.startswith("_"):
                            continue
                        funcs[name] = getattr(mod.Cmds, name)

                except Exception as e:
                    print(f"Error loading module {path}: {str(e)}", file=sys.stderr)
                    continue

    finally:
        # Restore sys.path
        sys.path.remove(dirpath)

    return funcs




def timed_input(prompt, timeout):
    def input_thread():
        nonlocal result
        try:
            result.append(input(prompt))
        except: # noqa
            pass

    result = []
    thread = threading.Thread(target=input_thread)
    thread.daemon = True
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        # Forcefully terminate the thread
        thread_id = ctypes.c_long(thread.ident)
        exception = ctypes.py_object(SystemExit)
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, exception)
        return None
    return result[0] if result else None