"""Shared helpers for the test suite.

mediola2mqtt.py loads its config at import time (and would call sys.exit if no
config were found), so these helpers prepare a minimal yaml in a temporary
working directory before the module is imported.
"""

import importlib
import os
import sys
import tempfile

_ORIGINAL_CWD = os.getcwd()
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))


_MINIMAL_YAML = """\
mediola:
  - host: 192.0.2.10
    id: mediola1
  - host: 192.0.2.20
    id: mediola2

mqtt:
  host: localhost
  port: 1883
  discovery_prefix: homeassistant
  topic: mediola
  debug: false

buttons:
  - type: IT
    adr: 3D5E00
    mediola: mediola1

switches:
  - type: IT
    adr: A01
    mediola: mediola1
  - type: IT
    name: Lamp
    on_value: "3C5B1490"
    off_value: "3C5B1480"
    mediola: mediola1

blinds:
  - type: ER
    adr: "01"
    name: TestER
    mediola: mediola2
  - type: RT
    adr: 5a25d5
    name: TestRT
    mediola: mediola1
"""


def load_module(yaml_text=_MINIMAL_YAML):
    """Import (or reload) mediola2mqtt with a controlled config in scope."""
    workdir = tempfile.mkdtemp(prefix='mediola2mqtt-tests-')
    yaml_path = os.path.join(workdir, 'mediola2mqtt.yaml')
    with open(yaml_path, 'w') as fh:
        fh.write(yaml_text)
    os.chdir(workdir)
    if _PROJECT_ROOT not in sys.path:
        sys.path.insert(0, _PROJECT_ROOT)
    if 'mediola2mqtt' in sys.modules:
        del sys.modules['mediola2mqtt']
    module = importlib.import_module('mediola2mqtt')
    os.chdir(_ORIGINAL_CWD)
    return module
