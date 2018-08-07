"""
need commands:
- upload to cdstar
- merge excel sheet
- to_mysql
- create offline version (per study)

legacy:
- to_cldf
- mysql2translations
"""
import os
import sys
import pathlib
from collections import OrderedDict
import json
import codecs
import shutil
import errno
import requests
import zipfile

import re
from pathlib import Path
from clldutils import jsonlib

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

from clldutils.clilib import ArgumentParserWithLogging, command
from clldutils.dsv import UnicodeWriter
from cdstarcat import Catalog

from pysoundcomparisons.api import SoundComparisons
from pysoundcomparisons.db import DB

def _db(args):
    return DB(host=args.db_host, db=args.db_name, user=args.db_user, password=args.db_password)


def _api(args):
    return SoundComparisons(repos=args.repos)


def _get_all_study_names(db):
    return [s['Name'] for s in list(db("select Name from Studies"))]


def _write_csv_to_file(data, file_name, api, header=None, dir_name='cldf'):
    outdir = api.repos.joinpath(dir_name)
    if not outdir.exists():
        outdir.mkdir()
    if header is None:
        try:
            header = data.keys()
        except AttributeError:
            pass
    with UnicodeWriter(outdir.joinpath(file_name)) as w:
        if header is not None:
            w.writerow(header)
        for row in data:
            w.writerow(row)

def _get_jsonparsed_data(url):
    """
    Receive the content of url, parse it as JSON and return the object.
    """
    repsonse = None
    try:
        response = urlopen(url)
        if response is None:
            return None
        data = response.read().decode("utf-8")
        return json.loads(data)
    except:
        return None

def _delete_folder(path):
    pth = Path(path)
    for sub in pth.iterdir():
        if sub.is_dir():
            _delete_folder(sub)
        else:
            sub.unlink()
    pth.rmdir()

def _copy_path(src, dest):
    try:
        shutil.copytree(src, dest)
    except OSError as e:
        # If the error was caused due to the source wasn't a folder
        # then copy a file
        if e.errno == errno.ENOTDIR:
            shutil.copy(src, dest)
        else:
            print('Directory not copied. Error: %s' % e)

def _copy_save_url(url, query, dest, file_path, api):
    response = None
    try:
        response = urlopen(url + "/" + query)
    except:
        _delete_folder(outPath)
        print("Please check first argument or connection for a valid URL %s" % (query))
        return False
    if response is None:
        _delete_folder(outPath)
        print("Please check first argument or connection for a valid URL %s" % (query))
        return False
    with open(api.repos.joinpath(dest, file_path), "wb") as output:
        output.write(response.read())
    return True

def _fetch_save_scdata_json(url, dest, file_path, prefix, api):
    data = requests.get(url).json()
    r = re.compile(r"http://cdstar[^/]*?/[^/]*?/[^/]*?/((.*?)_\d{3,}.*?\.)")
    with open(api.repos.joinpath(dest, "data", file_path + ".js"), "w") as output:
        output.write(prefix + r.sub(r"sound/\g<2>/\g<1>", json.dumps(data, separators=(',', ':'))))
    return data


@command()
def create_offline_version(args):
    """
    Creates sndComp_offline.zip without map tile files and (as default) without sound files.
    Usage:
      --sc-host --sc-repo createOfflineVersion
        sc-host: URL to soundcomparisons - default http://www.soundcomaprisons.com
        sc-repo: path to local Sound-Comparisons github repository - default './../../../Sound-Comparisons'

    Optional arguments (not yet implemented coming soon...)
      with_online_soundpaths  - use online cdstar sound paths instead of local ones (mainly for testing)
      all_sounds  - creates the sound folder and copy all mp3 and ogg sound files
      [any_stduy_name]  - creates the sound folder and copy all mp3 and ogg sound files of the passed study or studies
    """

    api = _api(args)

    outPath = "sndComp_offline"
    homeURL = args.sc_host
    baseURL = homeURL + "/query"
    sndCompRepoPath = args.sc_repo
    if (not os.path.exists(sndCompRepoPath)):
        print("Please check second argument '%s' for a valid Sound-Comparisons repository path."
            % (sndCompRepoPath))
        return

    # create folder structure
    if (os.path.exists(outPath)):
        _delete_folder(outPath)
    os.makedirs(outPath)
    os.makedirs(api.repos.joinpath(outPath, "data"))
    os.makedirs(api.repos.joinpath(outPath, "js"))
    os.makedirs(api.repos.joinpath(outPath, "js", "extern"))

    # copy from repo all necessary static files
    _copy_path(api.repos.joinpath(sndCompRepoPath, "site", "css"), api.repos.joinpath(outPath, "css"))
    _copy_path(api.repos.joinpath(sndCompRepoPath, "site", "img"), api.repos.joinpath(outPath, "img"))
    _copy_path(api.repos.joinpath(
        sndCompRepoPath, "site", "js", "extern", "FileSaver.js"),
        api.repos.joinpath(outPath, "js", "extern"))
    _copy_path(api.repos.joinpath(sndCompRepoPath, "LICENSE"), outPath)
    _copy_path(api.repos.joinpath(sndCompRepoPath, "README.md"), outPath)

    # create index.html - handle and copy the main App.js file
    response = None
    minifiedKey = ""
    try:
        response = urlopen(homeURL + "/index.html")
    except:
        _delete_folder(outPath)
        print("Please check first argument or connection for a valid URL (index.html)")
        return
    if response is None:
        _delete_folder(outPath)
        print("Please check first argument or connection for a valid URL (index.html)")
        return
    with open(api.repos.joinpath(outPath, "index.html"), "w") as output:
        data = response.read().decode("utf-8").splitlines(True)
        p = re.compile("(.*?)(App\\-minified)\\.(.*?)(\\.js)(.*)")
        for line in data:
            if p.match(line):
                g = p.match(line).groups()
                if not len(g) == 5:
                    _delete_folder(outPath)
                    print("Error while parsing index.html")
                    return
                output.write(g[0] + g[1] + g[3] + g[4] + "\n")
                minifiedKey = g[2]
            else:
                output.write(line)
    if not len(minifiedKey):
        _delete_folder(outPath)
        print("Error while getting minified key in index.html")
        return
    _copy_save_url(homeURL, "js/App-minified." + minifiedKey + ".js", outPath,
        api.repos.joinpath("js", "App-minified.js"), api)

    # get data global json
    _fetch_save_scdata_json(baseURL + "/data", outPath, "data", "var localData=", api)
    global_data = _fetch_save_scdata_json(
        baseURL + "/data?global", outPath, "data_global", "var localDataGlobal=", api)

    # get all study names out of global_data and query all relevant json files
    # and save them as valid javascript files which can be loaded via <script>...</script>
    all_studies = []
    try:
        all_studies = global_data['studies']
    except:
        _delete_folder(outPath)
        print("Error while getting all studies from global json.")
        return

    for s in all_studies:
        if(s != '--'): # skip delimiters
            _fetch_save_scdata_json(baseURL + "/data?study=" + s, outPath,
                "data_study_" + s, "var localDataStudy" + s + "=", api)

    # Providing translation files:
    tdata = _fetch_save_scdata_json(
        baseURL + "/translations?action=summary", outPath,
        "translations_action_summary", "var localTranslationsActionSummary=", api);
    # Combined translations map for all BrowserMatch:
    lnames = []
    for k in tdata.keys():
        lnames.append(tdata[k]['BrowserMatch'])
    _fetch_save_scdata_json(
            baseURL + "/translations?lng=" + "+".join(lnames) + "&ns=translation", outPath,
            "translations_i18n", "var localTranslationsI18n=", api);

    # create the zip archive
    try:
        zipf = zipfile.ZipFile(outPath + ".zip", "w", zipfile.ZIP_DEFLATED)
        fp = os.path.join(outPath, "..")
        for root, dirs, files in os.walk(outPath):
            for f in files:
                if not f.startswith(".") and not f.startswith("__"): # mainly for macOSX hidden files
                    zipf.write(os.path.relpath(os.path.join(root, f), fp))
        zipf.close()
        _delete_folder(outPath)
        print("done")
    except Exception as e:
        print("Something went wrong while creating the zip archive.")
        print(e)

@command()
def write_modified_soundfiles(args):
    """
    Creates the file 'modified.json' in folder 'soundfiles' which contains the following keys:
    • 'new': paths on soundcomparisons.com which are not on cdstar
    • 'modified': paths on cdstar whose source on soundcomparisons.com had changed
    • 'obsolete': paths on cdstar which are not on soundcomparisons.com and not valid paths
    • 'check': paths on cdstar which are not on soundcomparisons.com BUT VALID paths
        [paths which are probably deleted by mistake on soundcomparisons.com ]
    - files needed before execution:
    • 'ServerSndFilesChecksums.txt' in 'pysoundcomparisons' - generate via:
        find /srv/soundcomparisons/site/sound/ -iname "*[.wav\\|.mp3\\|.ogg]" -type f -exec md5sum {} \\; > ServerSndFilesChecksums.txt
      at soundcomparisons.com server
    • 'valid_soundfilepaths.txt' in 'pysoundcomparisons' - generate via 'write_valid_soundfilepaths'
    """

    api = _api(args)

    catalog_items = {}
    return_data = {}
    return_new = set()
    return_modified = {}
    return_obsolete = {}
    return_check = {}
    cdstar_object_metadata = {}
    valid_soundfilepaths = []
    server_md5_items = set()

    valid_soundfilepaths_filepath = api.repos.joinpath(
        'valid_soundfilepaths.txt')
    if valid_soundfilepaths_filepath.exists():
        with open(valid_soundfilepaths_filepath) as fp:
            line = fp.readline().strip()
            while line:
                lineArray = line.split("/")
                if len(lineArray) > 0:
                    valid_soundfilepaths.append(lineArray[-1])
                line = fp.readline().strip()

    catalog_filepath = api.repos.joinpath(
        '..', 'soundfiles', 'catalog.json')
    if catalog_filepath.exists():
        catalog_items = jsonlib.load(catalog_filepath)
    else:
        print("File path {} does not exist.".format(catalog_filepath))
        return return_data

    server_md5_filepath = api.repos.joinpath(
        '..', 'soundfiles', 'ServerSndFilesChecksums.txt')
    if not os.path.isfile(server_md5_filepath):
        print("File path {} does not exist. Please generate it first.".format(server_md5_filepath))
        return return_data

    # Load cached metadata in order to minimize cdstar server lookups
    # If not desired simply delete or rename catalog_metatdata.json
    cdstar_object_metadata_filepath = api.repos.joinpath(
        '..', 'soundfiles', 'catalog_metatdata.json')
    if cdstar_object_metadata_filepath.exists():
        cdstar_object_metadata = jsonlib.load(cdstar_object_metadata_filepath)

    cnt = 1
    with open(server_md5_filepath) as fp:
        line = fp.readline().strip()
        while line:
            (md5, sffolder, sfpath, ext) = re.match(
                r"^(.*?)  .*/([^/]+?)/([^/]+?)\.(.*)", line).groups()
            server_md5_items.add(sfpath)
            # if sfpath.startswith("Oce_"):
            if sfpath in catalog_items:
                meta_obj = None
                if sfpath in cdstar_object_metadata:
                    meta_obj = cdstar_object_metadata[sfpath]
                else:
                    meta_obj = _get_jsonparsed_data(
                        "http://cdstar.shh.mpg.de/objects/%s" % (catalog_items[sfpath][0])
                    )['bitstream']
                    if meta_obj is None:
                        with open(cdstar_object_metadata_filepath, 'w') as f:
                            json.dump(cdstar_object_metadata, f, indent=4)
                        print("HTTP Error - wait a sec and rerun the command")
                        sys.exit()
                    else:
                        cdstar_object_metadata[sfpath] = meta_obj
                if meta_obj is not None:
                    check_sf = "%s.%s" % (sfpath, ext)
                    found = False
                    for sf in meta_obj:
                        if sf['bitstreamid'] == check_sf:
                            found = True
                            if sf['checksum'] == md5:
                                pass
                            else:
                                if catalog_items[sfpath][0] not in return_modified:
                                    return_modified[catalog_items[sfpath][0]] = []
                                return_modified[catalog_items[sfpath][0]].append(
                                    "%s/%s" % (sffolder, check_sf)
                                    )
                            break
                    if not found and sfpath in valid_soundfilepaths:
                        return_new.add("%s/%s" % (sffolder, check_sf))
                else:
                    print("Error while processing %s for %s" % (
                        sfpath, catalog_items[sfpath][0]), flush=True)
            else:
                if sfpath in valid_soundfilepaths:
                    return_new.add("%s/%s" % (sffolder, sfpath))

            cnt += 1
            if not cnt % 10000:
                # Save metadata cache in case cdstar request fails
                # to avoid querying again
                with open(cdstar_object_metadata_filepath, 'w') as f:
                    json.dump(cdstar_object_metadata, f, indent=4)

            line = fp.readline().strip()

    # Save metadata cache
    with open(cdstar_object_metadata_filepath, 'w') as f:
        json.dump(cdstar_object_metadata, f, indent=4)

    # Check if there are items in catalog.json
    # which are not listed in ServerSndFilesChecksums.txt 
    # and distinguish between valid paths.
    # 'obsolete' objects could be deleted, 'check' data are valid on cdstar 
    #   but not on soundcomparisons.com !
    for k in catalog_items.keys():
        if k not in server_md5_items:
            if k in valid_soundfilepaths:
                if catalog_items[k][0] not in return_check:
                    return_check[catalog_items[k][0]] = []
                return_check[catalog_items[k][0]].append(k)
            else:
                if catalog_items[k][0] not in return_obsolete:
                    return_obsolete[catalog_items[k][0]] = []
                return_obsolete[catalog_items[k][0]].append(k)

    return_data = {
        'new': sorted(return_new),
        'modified': return_modified,
        'obsolete': return_obsolete,
        'check': return_check
    }

    with open(api.repos.joinpath('..', 'soundfiles', 'modified.json'), 'w') as f:
        json.dump(return_data, f, indent=4)

@command()
def write_languages(args):
    """
    Get all unique language data from all studies (Languages_*) and
    write them into file 'languages.csv' and the mapping between
    language and study into x_study_languages.csv'. Before writing files
    it will be checked if any language data differ across studies.
    """
    db = _db(args)
    api = _api(args)

    all_studies = _get_all_study_names(db)
    union_query_array = []
    union_query_withstudy_array = []
    for study in all_studies:
        union_query_array.append("SELECT * FROM Languages_%s" % (study))
        union_query_withstudy_array.append(
            "SELECT *, '%s' AS Study FROM Languages_%s" % (study, study))

    # first check for language uniqueness across studies
    query = """SELECT DISTINCT LanguageIx, count(*) AS c FROM (%s) AS t
        GROUP BY LanguageIx HAVING c > 1""" % (
        " UNION ".join(union_query_array))
    data = list(db(query))
    if len(data) > 0:
        print(
            "\nData of these languages differ across studies - please clean up data first:")
        for row in data:
            print("\n")
            for study in all_studies:
                query = """SELECT LanguageIx, ShortName FROM Languages_%s
                    WHERE LanguageIx = %s""" % (study, row['LanguageIx'])
                qdata = list(db(query))
                if len(qdata) > 0:
                    print("LanguageIx = %s (%s) in study %s" % (
                        qdata[0]['LanguageIx'], qdata[0]['ShortName'], study))
        return

    # make sure all studies will be concatenated
    db("SET @@group_concat_max_len = 4096")
    query = """SELECT DISTINCT *, GROUP_CONCAT(Study) AS Studies FROM (%s) AS t
        GROUP BY LanguageIx""" % (" UNION ".join(union_query_withstudy_array))
    data = db(query)
    # header minus last two columns Study and Studies
    header = data.keys()[:-2]

    # go through each row, get mapping LanguageIx and Study and 
    # delete the last two columns
    data_db = list()
    study_lg_map_data = list()
    for row in data:
        data_db.append(row[:-2])
        for s in row['Studies'].split(","):
            study_lg_map_data.append([row['LanguageIx'], s])

    _write_csv_to_file(data_db, 'languages.csv', api, header)
    _write_csv_to_file(study_lg_map_data, 'x_study_languages.csv', api, [
        'LanguageIx', 'StudyName'])

@command()
def write_valid_soundfolders(args):
    """
    Creates the file 'valid_soundfolderrs.txt' containing all valid
    sound file folder based on database data. With the command line
    option --only-for-study <STUDY_NAME> the output file will list only
    those folder names which are relevant for STUDY_NAME.
    """
    db = _db(args)
    all_studies = _get_all_study_names(db)
    if args.only_for_study is not None and args.only_for_study in all_studies:
        all_studies = [args.only_for_study]
    valid_folders = set()
    for s in all_studies:
        q = "SELECT DISTINCT FilePathPart FROM Languages_%s" % (s)
        data = list(db(q))
        if len(data) > 0:
            for row in data:
                valid_folders.add(row['FilePathPart'])
    with codecs.open("valid_soundfolders.txt", "w", "utf-8-sig") as f:
        f.write("\n".join(sorted(valid_folders)))
        f.close()


@command()
def write_valid_soundfilepaths(args):
    """
    Creates the file 'valid_soundfilepaths.txt' containig all valid
    sound file paths based on database data.
    """
    db = _db(args)
    all_studies = _get_all_study_names(db)
    union_query_withstudy_array = []
    for study in all_studies:
        union_query_withstudy_array.append(
            "SELECT *, '%s' AS Study FROM Languages_%s" % (study, study))

    # make sure all studies will be concatenated
    db("SET @@group_concat_max_len = 4096")
    query = """SELECT DISTINCT FilePathPart, LanguageIx, GROUP_CONCAT(Study) AS Studies
        FROM (%s) AS t GROUP BY FilePathPart""" % (
        " UNION ".join(union_query_withstudy_array))
    data = list(db(query))
    valid_snd_file_names = set()
    for row in data:
        for s in row['Studies'].split(","):
            q = """SELECT DISTINCT 
                SoundFileWordIdentifierText, IxElicitation, IxMorphologicalInstance
                FROM Words_%s""" % (s)
            d = list(db(q))
            for r in d:
                q2 = """SELECT DISTINCT
                    AlternativePhoneticRealisationIx AS P, AlternativeLexemIx AS L FROM Transcriptions_%s
                    WHERE LanguageIx = %s
                    AND IxElicitation = %s
                    AND IxMorphologicalInstance = %s""" % (
                        s, row['LanguageIx'], r['IxElicitation'], r['IxMorphologicalInstance'])
                d2 = list(db(q2))
                fileNamePrefix = row['FilePathPart'] + "/" + row['FilePathPart'] + r['SoundFileWordIdentifierText']
                valid_snd_file_names.add(fileNamePrefix)
                existsBasis = False
                for t in d2:
                    if t['P'] == 1 or t['L'] == 1:
                        print("check %s = %s IxElic %s for AltPhon |& AltLex = 1" % (
                            row['LanguageIx'], row['FilePathPart'], r['IxElicitation']))
                    if t['P'] == 0 and t['L'] == 0:
                        existsBasis = True
                    if t['P'] == 0 and t['L'] > 1:
                        valid_snd_file_names.add(fileNamePrefix + "_lex%s" % (t['L']))
                    elif t['P'] > 1 and t['L'] > 1:
                        valid_snd_file_names.add(fileNamePrefix + "_lex%s_pron%s" % (t['L'], t['P']))
                    elif t['P'] > 1 and t['L'] == 0:
                        valid_snd_file_names.add(fileNamePrefix + "_pron%s" % (t['P']))
                # if not existsBasis:
                #     print("check %s = %s IxElic %s no AltPhon = AltLex = 0" % (
                #         row['LanguageIx'], row['FilePathPart'], r['IxElicitation']))

    with codecs.open("valid_soundfilepaths.txt", "w", "utf-8-sig") as f:
        f.write("\n".join(sorted(valid_snd_file_names)))
        f.close()


@command()
def write_translations(args):
    db = _db(args)
    api = _api(args)
    for row in list(db("select * from Page_Translations")):
        data = OrderedDict()
        #if row['Active']:
        print(row['TranslationName'], row['Active'])
        for tr in db(
            "select Req, Trans, IsHtml from Page_StaticTranslation where TranslationId = %s order by Req",
            (row['TranslationId'],)
        ):
            data[tr['Req']] = tr['Trans']
        print(len(data))
        for tr in db(
                "select Category, Field, Trans from Page_DynamicTranslation where TranslationId = %s order by Category, Field",
                (row['TranslationId'],)
        ):
            data[tr['Category'] + tr['Field']] = tr['Trans']
        print(len(data))

        outdir = api.repos.joinpath('translations', row['BrowserMatch'])
        if not outdir.exists():
            outdir.mkdir()
        with outdir.joinpath('translations.json').open('w') as fp:
            json.dump(data, fp, indent=4)


@command()
def upload(args):
    from pysoundcomparisons.mediacatalog import MediaCatalog
    from pycdstar.api import Cdstar

    api = _api(args)

    with MediaCatalog(args.repos) as cat:
        cat.add(
            Cdstar(
                service_url=os.environ['CDSTAR_URL'],
                user=os.environ['CDSTAR_USER'],
                password=os.environ['CDSTAR_PWD']),
            args.args[0])

    return

    with Catalog(
        sfdir / 'catalog.json',
        cdstar_url=os.environ['CDSTAR_URL'],
        cdstar_user=os.environ['CDSTAR_USER'],
        cdstar_pwd=os.environ['CDSTAR_PWD']) as cat:
        for fname in sfdir.joinpath('upload').iterdir():
            if fname.is_file() and fname.name not in ['README', '.gitignore']:
                md = {
                    'collection': 'soundcomparisons',
                    'path': str(fname.relative_to(sfdir / 'upload')),
                }
                try:
                    _, _, obj = list(cat.create(fname, md, filter_=lambda f: True))[0]
                except:
                    print(fname)


def main():  # pragma: no cover
    parser = ArgumentParserWithLogging('pysoundcomparisons')
    parser.add_argument(
        '--repos',
        help="path to soundcomparisons-data repository",
        type=pathlib.Path,
        default=pathlib.Path(__file__).parent.parent)
    parser.add_argument('--db-host', default='localhost')
    parser.add_argument('--db-name', default='soundcomparisons')
    parser.add_argument('--db-user', default='soundcomparisons')
    parser.add_argument('--db-password', default='pwd')
    parser.add_argument('--sc-host', default='http://www.soundcomparisons.com')
    parser.add_argument('--sc-repo',
        type=pathlib.Path,
        default=os.path.join(
            pathlib.Path(__file__).resolve().parent.parent.parent,
            'Sound-Comparisons'))
    parser.add_argument('--only-for-study', default=None)
    sys.exit(parser.main())


if __name__ == '__main__':
    main()
