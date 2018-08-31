import os
import re
import attr
from pathlib import Path
from cdstarcat import Catalog
from clldutils.misc import lazyproperty


class SoundfileName(str):

    def __new__(cls, content):
        # split into variety|word_id|word_text|{extension}
        try:
            p = re.findall('(.+?)_(\d{3,})_([^\.]+)\.?(.+)?', content)[0]
        except IndexError:
            raise ValueError()
        s = str.__new__(cls, "_".join(p[:-1]))
        (s.variety, s.word_id, s.word_text, s.extension) = p
        return s

class MediaCatalog(Catalog):

    valid_mimetypes = {
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
        return { obj.metadata['name'] : obj for obj in self }

    def names_for_variety(self, prefix="", trailing_delimiter="_"):
        return [
            k for k in self._name_uid_map if k.startswith(prefix + trailing_delimiter)]

    def matching_bitstreams_for_mimetypes(self, obj, mimetypes=None):
            mimetypes = mimetypes or set(self.mimetypes.values())
            return [bs for bs in obj.bitstreams if bs.mimetype in mimetypes]

    def bitstream_url(self, obj, bs):
        return "%s/bitstreams/%s/%s" % (
            self.api.service_url or os.environ.get("CDSTAR_URL", "http://cdstar.shh.mpg.de"),
            obj.id,
            bs.id
        )
