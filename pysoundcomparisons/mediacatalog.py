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

    __uid_map = None

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

    def __getitem__(self, key):
        return self.items[key]

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        for key in self.items:
            yield key

    def keys(self):
        return list(self.items.keys())

    def values(self):
        return list(self.items.values())

    def add(self, api, d):
        valid_extensions = ["." + x for x in self.__mimetypes.keys()]
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
                        api, word, [f for f in files if f.suffix in valid_extensions])
                    self.__uid_map = None

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

    def file(self):
        return self.path

    def sound_extensions(self):
        return sorted(self.__mimetypes.keys())

    def keys_with_prefix(self, prefix="", trailing_delimiter="_"):
        return [key for key in self.items if key.startswith(prefix + trailing_delimiter)]

    def uid_for_key(self, item):
        return self[item][0]

    def key_for_uid(self, uid):
        if self.__uid_map is None:
            self.__uid_map = { self.items[x][0]: x for x in self.items }
        return self.__uid_map[uid]

    def uids(self):
        if self.__uid_map is None:
            self.__uid_map = { self.items[x][0]: x for x in self.items }
        return self.__uid_map.keys()

    def extensions_for_key(self, item):
        try:
            return self[item][1]
        except:
            return []

