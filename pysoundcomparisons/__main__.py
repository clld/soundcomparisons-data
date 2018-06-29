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
    sys.exit(parser.main())


if __name__ == '__main__':
    main()
