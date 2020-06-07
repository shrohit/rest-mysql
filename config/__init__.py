
import os
import importlib

from . import default


# Importing deployment.py if exists
deployment = None
curr_dir = os.path.dirname(os.path.abspath(__file__))
deployment_filepath = os.path.join(curr_dir, "deployment.py")
if os.path.exists(deployment_filepath):
    deployment = importlib.import_module(".deployment", __package__)


class Config(object):

    def __getattribute__(self, attr):
        if os.getenv(attr) is not None:      # Precedence-1: env variables
            return os.getenv(attr)
        if hasattr(deployment, attr):        # Precedence-2: deployment.py
            return getattr(deployment, attr)
        if hasattr(default, attr):           # Precedence-3: default.py
            return getattr(default, attr)
        raise KeyError("Configuration key missing: {}".format(attr))
