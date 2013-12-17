import iphoto
import json
import logging
import os.path
import sys
import re

logging.basicConfig(level=logging.INFO)
iphoto_path = sys.argv[1]

if not os.path.exists(iphoto_path):
    raise Exception('iPhoto is not found')

renamer = lambda old_name, path: re.sub(r'^#dupmark:[0-9]+:', '', old_name)
rnhelper = iphoto.RenamingHelper(iphoto_path)
rnhelper.rename_all(all_paths, renamer)
