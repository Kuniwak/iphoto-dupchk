import dupdetector
import logging
import os.path
import sys
import iphoto

logging.basicConfig(level=logging.INFO)
iphoto_path = sys.argv[1]

if not os.path.exists(iphoto_path):
    raise Exception('iPhoto is not found')

detector = iphoto.Dupdetector(iphoto_path)
detector.check()
