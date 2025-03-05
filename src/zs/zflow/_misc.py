
import logging

fileLogger = logging.getLogger("zflow")
fileLogger.addHandler(logging.FileHandler("zflow.log"))
fileLogger.propagate = False



