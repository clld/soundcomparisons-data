from collections import OrderedDict
from itertools import groupby
from pathlib import Path
import time

from clldutils import jsonlib


class MediaCatalog(object):
    __mimetypes = {
        'mp3': 'audio/mpeg',
        'ogg': 'audio/ogg',
        'wav': 'audio/wav',
    }

    def __init__(self, repos):
        self.path = Path(repos).joinpath('soundfiles', 'catalog.json')
        if self.path.exists():
            self.items = jsonlib.load(self.path, object_pairs_hook=OrderedDict)
        else:
            self.items = OrderedDict()

    def __contains__(self, item):
        return item in self.items

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        jsonlib.dump(self.items, self.path, indent=4)

    def add(self, api, d):
        for sd in sorted(Path(d).iterdir(), key=lambda p: p.name):
            if sd.is_dir():
                for word, files in groupby(
                    sorted(sd.iterdir(), key=lambda f: f.name),
                    lambda f: f.stem
                ):
                    #if word in self: # and not word.startswith(sd.name):
                    #    print('-->', word)
                    #    print(sd.name)
                    #    print(self.items[word][0])
                    if not word.startswith(sd.name):
                        continue
                    if word in self:
                        continue
                    print(word)
                    self.items[word] = self._add(
                        api, word, [f for f in files if f.suffix in ['.wav', '.ogg', '.mp3']])

    def _add(self, api, word, files):
        # create an object, add all files as bitstreams
        obj = api.get_object()
        formats = []
        try:
            obj.metadata = {
                'collection': 'soundcomparisons',
                'name': 'word',
                'type': 'soundfile',
            }
            for f in files:
                fmt = f.suffix[1:]
                obj.add_bitstream(fname=str(f), name=fmt, mimetype=self.__mimetypes[fmt])
                time.sleep(1.3)
                formats.append(fmt)
        except:  # noqa: E722
            obj.delete()
            raise
        return obj.id, sorted(formats)
