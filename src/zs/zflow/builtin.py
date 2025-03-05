import os

def sleep(x: str):
    from zuu.util_timeparse import time_parse
    import datetime
    sleep_till = time_parse(x)
    elapsed = sleep_till - datetime.datetime.now()
    if elapsed.total_seconds() > 0:
        os.system(f"timeout /t {int(elapsed.total_seconds())}")

def setenv(x: str):
    k, v = x.split("=", 1)
    os.environ[k] = v

BUILTIN_METHODS = {
    "sleep": sleep,
    "setenv": setenv,
}