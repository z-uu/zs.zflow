import datetime
import logging
import os
import uuid
from dataclasses import dataclass, field
import typing
from zuu.util_timeparse import time_parse
import bisect
import yaml

from ._misc import fileLogger

class Expired(Exception):
    pass

class When(typing.TypedDict):
    minAt : typing.Union[int, str] = None
    maxAt : typing.Union[int, str] = None

STEPTYPE = typing.Union[dict, str]

@dataclass(slots=True)
class ZFlowTask:
    name : str
    when : When
    id : str = None
    description : typing.Optional[str] = None
    cleanup : bool = False
    lifetime : typing.Optional[int] = None

    init : typing.List[STEPTYPE] = field(default_factory=list)
    steps : typing.List[STEPTYPE] = field(default_factory=list)
    onError : typing.List[STEPTYPE] = field(default_factory=list)

    _minAt : datetime.datetime = None
    _maxAt : datetime.datetime = None

    def __post_init__(self):
        assert self.when is not None and isinstance(self.when, dict), "when is required"

        if self.id is None:
            self.id = str(uuid.uuid4())

        if self.lifetime is not None and "minAt" in self.when and "maxAt" in self.when:
            # check if lifetime is within minAt and maxAt, lifetime is int in seconds
            lifetime = datetime.timedelta(seconds=self.lifetime)
            if self.minAt + lifetime > self.maxAt:
                raise ValueError("lifetime cannot fit within minAt and maxAt")

    @property
    def minAt(self):
        if self._minAt is None and "minAt" in self.when:     
            self._minAt = time_parse(self.when["minAt"])
            if self._minAt.date() != datetime.datetime.now().date():
                raise Expired("minAt must be within the current day")
        return self._minAt

    @property
    def maxAt(self):
        if self._maxAt is None and "maxAt" in self.when: 
            self._maxAt = time_parse(self.when["maxAt"])
            if self._maxAt.date() != datetime.datetime.now().date():
                raise Expired("maxAt must be within the current day")
        return self._maxAt

    @property
    def canRun(self):
        now = datetime.datetime.now()
        if self.minAt is not None and now < self.minAt:
            return False
        if self.maxAt is not None and now > self.maxAt:
            return False
        return True

class ZFlowQueue:
    def __init__(self):
        self._queue = []

    def enqueue(self, task: ZFlowTask):
        item = (task.minAt, task.maxAt, task)
        bisect.insort(self._queue, item)

    def dequeue(self):
        return self._queue.pop(0) if self._queue else None
    
    def peek(self) -> typing.Optional[tuple[datetime.datetime, datetime.datetime, ZFlowTask]]:
        return self._queue[0] if self._queue else None

    def from_file(self, file : str):
        if not os.path.exists(file):
            raise FileNotFoundError(f"File {file} not found")
        
        if not file.endswith(".yml") and not file.endswith(".yaml"):
            raise ValueError("File must end with .yml or .yaml")
        
        with open(file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        try:
            obj = ZFlowTask(**data)
            self.enqueue(obj)
        except Expired as e:
            logging.error(f"{file} is expired: {e}")
        except Exception as e:
            logging.error(f"failed to parse {file}: {e}")
            fileLogger.error(f"failed to parse {file}: {e}")
            # traceback
            import traceback
            fileLogger.error(traceback.format_exc())

    def from_folder(self, folder : str):
        if not os.path.exists(folder):
            raise FileNotFoundError(f"Folder {folder} not found")
        
        for file in os.listdir(folder):
            if file.endswith(".yml") or file.endswith(".yaml"):
                self.from_file(os.path.join(folder, file))
