from cdstarcat import Catalog

class MediaCatalog(Catalog):
    __mimetypes = {
        'mp3': 'audio/mpeg',
        'ogg': 'audio/ogg',
        'wav': 'audio/wav',
    }

    __file_path_uid_map = None

    def file_path_uid_map(self):
        if self.__file_path_uid_map is None:
            self.__file_path_uid_map = { 
                self.objects[i].metadata['name'] : i for i in self.objects}
        return self.__file_path_uid_map

    def file_path_keys_with_prefix(self, prefix="", trailing_delimiter="_"):
        return [
            k for k in self.file_path_uid_map() if k.startswith(prefix + trailing_delimiter)]

    def sound_extensions(self):
        return sorted(self.__mimetypes.keys())

    def extensions_for_file_path_key(self, fpath):
        ret = []
        for bs in self.objects[self.file_path_uid_map()[fpath]].bitstreams:
            for (ext, mime) in self.__mimetypes.items():
                if bs.mimetype == mime:
                    ret.append(ext)
        return ret

