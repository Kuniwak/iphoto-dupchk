import iphoto
import json
import logging
import os.path
import sys

logging.basicConfig(level=logging.INFO)
iphoto_path = sys.argv[1]
dup_list_path = sys.argv[2]

if not os.path.exists(iphoto_path):
    raise Exception('iPhoto is not found')

with open(dup_list_path) as f:
    logging.info('Scanning a given list for duplicated files...')

    dup_bucket = []
    for line in f.readlines():
        dup_bucket.append(json.loads(line))

    logging.info('Done.')

all_paths = set()
dup_group_map = {}

max = len(dup_bucket)
max_digit = len(str(max))
for i in range(max):
    dup_group = dup_bucket[i]

    for path in dup_group:
        all_paths.add(path)
        dup_group_map[path] = str(i).zfill(max_digit)

flaghelper = iphoto.FlagHelper(iphoto_path)
flaghelper.add_all(paths)

renamer = lambda old_name, path: '#dupmark:{0}:{1}'.format(dup_group_map[path], old_name)
rnhelper = iphoto.RenamingHelper(iphoto_path)
rnhelper.rename_all(all_paths, renamer)
