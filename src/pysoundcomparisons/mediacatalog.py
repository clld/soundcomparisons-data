import re
from itertools import groupby
from pathlib import Path
import time

from cdstarcat import Catalog, Object
from clldutils.misc import lazyproperty
from clldutils.path import md5

__all__ = ['SoundfileName', 'MediaCatalog']


class SoundfileName(str):
    pattern = re.compile(
        '(?P<variety>.+?)_(?P<word_id>\d{3,})_(?P<word>[^.]+)\.?(?P<extension>.+)?')

    def __new__(cls, content):
        # split into variety|word_id|word_text|{extension}
        match = cls.pattern.search(content)
        if not match:
            raise ValueError('invalid {0}: {1}'.format(cls.__name__, content))

        s = str.__new__(cls, '_'.join(match.groups()[:-1]))
        for k, v in match.groupdict().items():
            setattr(s, k, v)
        return s

    @property
    def path(self):
        return Path('{0}.{0.extension}'.format(self))


class MediaCatalog(Catalog):

    mimetypes = {
        'mp3': 'audio/mpeg',
        'ogg': 'audio/ogg',
        'wav': 'audio/wav',
    }

    def __getitem__(self, key):
        """
        Return the object identified by UID or a file path.
        """
        return self.objects.get(key) or self._name_uid_map.get(key)

    def __contains__(self, item):
        """
        Check whether an UID or a file path is in the catalog.
        """
        return (item in self.objects) or (item in self._name_uid_map)

    @lazyproperty
    def _name_uid_map(self):
        return {obj.metadata['name']: obj for obj in self}

    def get_soundfilenames(self, prefix=""):
        return [k for k in self._name_uid_map if k.startswith(prefix)]

    def matching_bitstreams(self, obj, mimetypes=None):
        if not isinstance(obj, Object):
            obj = self[obj]
        mimetypes = mimetypes or set(self.mimetypes.values())
        return [bs for bs in obj.bitstreams if bs.mimetype in mimetypes] or [obj.bitstreams[0]]

    def bitstream_url(self, obj, bs):
        return self.api.url("/bitstreams/%s/%s" % (obj.id, bs.id))

    def _upload(self, sfn, files):
        """
        Upload a files for SoundfileName sfn.
        """
        print(sfn)
        # Lookup the SoundfileName in catalog:
        cat_obj = self[sfn] if sfn in self else None
        # Retrieve or create the corresponding CDSTAR object:
        obj = self.api.get_object(cat_obj.id if cat_obj else None)
        print(obj.id)
        md = {'collection': 'soundcomparisons', 'name': sfn, 'type': 'soundfile'}
        changed = False
        if not cat_obj:  # If the object is already in the catalog, the metadata does not change!
            obj.metadata = md
        for f in files:
            fmt = f.suffix[1:]
            if fmt not in self.mimetypes:
                continue
            create = True
            if cat_obj:
                for cat_bitstream in cat_obj.bitstreams:
                    if cat_bitstream.id.endswith(f.suffix):
                        # A bitstream for this mimetype already exists!
                        if cat_bitstream.md5 == md5(f):
                            # If the md5 sum is the same, don't bother uploading!
                            create = False
                        else:
                            # Otherwise we have to delete the old bitstream before uploading the
                            # new one.
                            for bs in obj.bitstreams:
                                if bs.id == cat_bitstream.id:
                                    bs.delete()
                                    break
                        break

            if create:
                changed = True
                print('uploading {0}'.format(f.name))
                obj.add_bitstream(fname=str(f), name=f.name, mimetype=self.mimetypes[fmt])
                time.sleep(0.1)
            else:
                print('skipping {0}'.format(f.name))

        if changed:
            obj.read()
            self.add(obj, metadata=md, update=True)

    def upload(self, d):
        """
        Upload files matching SoundfileName in directory d to CDSTAR
        """
        for stem, files in groupby(sorted(d.iterdir(), key=lambda f: f.name), lambda f: f.stem):
            try:
                self._upload(SoundfileName(stem), files)
            except ValueError:
                pass
