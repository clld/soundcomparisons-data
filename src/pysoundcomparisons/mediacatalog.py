import re
from pathlib import Path
from cdstarcat import Catalog, Object
from clldutils.misc import lazyproperty

__all__ = ['SoundfileName', 'MediaCatalog']


class SoundfileName(str):
    pattern = re.compile(
        '(?P<variety>.+?)_(?P<word_id>\d{3})_(?P<word>[^.]+)\.?(?P<extension>.+)?')

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
