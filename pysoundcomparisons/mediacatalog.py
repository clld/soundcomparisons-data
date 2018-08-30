from cdstarcat import Catalog
from clldutils.misc import lazyproperty

class MediaCatalog(Catalog):

    valid_mimetypes = {
        'mp3': 'audio/mpeg',
        'ogg': 'audio/ogg',
        'wav': 'audio/wav',
    }

    @lazyproperty
    def file_path_uid_map(self):
        return {obj.metadata['name'] : obj.id for obj in self}

    def file_path_keys_with_prefix(self, prefix="", trailing_delimiter="_"):
        return [
            k for k in self.file_path_uid_map if k.startswith(prefix + trailing_delimiter)]

    def extensions_for_file_path_key(self, fpath):
        ret = []
        for bs in self.objects[self.file_path_uid_map[fpath]].bitstreams:
            for (ext, mime) in self.valid_mimetypes.items():
                if bs.mimetype == mime:
                    ret.append(ext)
        return ret

