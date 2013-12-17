import hashlib
import json
import logging
import os
import os.path
import progressbar


class Dupdetector:
    ALLOWED_EXTENSIONS = set(['.jpeg', '.jpg', '.png'])

    def __init__(self, iphoto_path):
        self.hashdict = {}
        self.iphoto_path = iphoto_path
        if not os.path.exists(self.iphoto_path):
            raise Exception('iPhoto not found' + self.iphoto_path)

        self.masters_path = os.path.join(iphoto_path, 'Masters')
        if not os.path.exists(self.masters_path):
            raise Exception('Masters not found: ' + self.masters_path)

        self._built = False

    def digest(self, path):
        return self._md5(path)

    def _md5(self, path):
        with open(path, 'rb') as f:
            hexdigest = hashlib.md5(f.read()).hexdigest()
        return hexdigest

    def memorize(self, path, digest):
        dupset = self.hashdict.setdefault(digest, set())
        dupset.add(path)

    def get_image_paths(self):
        result = set()
        for dirpath, dirnames, filenames in os.walk(self.masters_path):
            for filename in filenames:
                ext = os.path.splitext(filename)[1].lower()
                if ext in Dupdetector.ALLOWED_EXTENSIONS:
                    path = os.path.join(dirpath, filename)
                    result.add(path)

        return result

    def _build_hashdict(self):
        if self._built:
            raise Exception('Already built')

        logging.info('Scanning images...')

        image_paths = self.get_image_paths()
        image_count = len(image_paths)

        pbar_options = {
            'widgets': [progressbar.Counter(), ' / ' + str(image_count) + ' ', progressbar.Bar()],
            'maxval': image_count
        }
        pbar = progressbar.ProgressBar(**pbar_options).start()

        i = 0
        for image_path in image_paths:
            self.memorize(image_path, self.digest(image_path))
            i += 1
            pbar.update(i)

        pbar.finish()
        self._built = True

    def _get_duplicateds(self):
        if not self._built:
            raise Exception('Not built yet')

        result = []

        for digest, dupset in self.hashdict.items():
            if len(dupset) > 1:
                result.append(dupset)

        return result

    def check(self):
        self._build_hashdict()

        for dupset in self._get_duplicateds():
            print(json.dumps(list(dupset)))
