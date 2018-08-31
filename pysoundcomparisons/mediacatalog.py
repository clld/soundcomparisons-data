from cdstarcat import Catalog
from clldutils.misc import lazyproperty

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
        return self.objects.get(key) or self.file_path_uid_map.get(key)

    def __contains__(self, item):
        """
        Check whether an UID or a file path is in the catalog.
        """
        return (item in self.objects) or (item in self.file_path_uid_map)

    @lazyproperty
    def file_path_uid_map(self):
        return { obj.metadata['name'] : obj for obj in self }

    def file_path_keys_with_prefix(self, prefix="", trailing_delimiter="_"):
        return [
            k for k in self.file_path_uid_map if k.startswith(prefix + trailing_delimiter)]

    def matching_bitstreams_for_mimetypes(self, sfpath, mimetypes=None):
            mimetypes = mimetypes or set(self.mimetypes.values())
            return [bs for bs in self[sfpath].bitstreams if bs.mimetype in mimetypes]
