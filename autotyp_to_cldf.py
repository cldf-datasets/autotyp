#!/usr/bin/env python3
# autotyp_to_cldf.py

import csv
import pathlib
from pprint import pprint

import yaml
import pandas as pd

import csvw
import pycldf

OUT_DIR = pathlib.Path('cldf')

META_DIR = pathlib.Path('metadata')
DATA_DIR = pathlib.Path('data')

LANG_META = META_DIR / 'Register.yaml'
LANG = DATA_DIR / 'Register.csv'

METADATA = sorted(p for p in META_DIR.glob('*.yaml') if p != LANG_META)
DATA = sorted(p for p in DATA_DIR.glob('*.csv') if p != LANG)

ENCODING = 'utf-8'

METACOLS = [
    'Module', 'Variable',
    'VariantOf',
    'SetUp', 'DataEntry', 'VariableType', 'DataType',
    'N.levels', 'N.entries', 'N.languages', 'N.missing',
    'Levels',
    'Description', 'Notes',
]

CLDF = 'http://cldf.clld.org/v1.0/terms.rdf#'


def yaml_load(filepath):
    with filepath.open('rb') as f:
        doc = yaml.safe_load(f)
    return doc


def csv_header(filepath, encoding=ENCODING):
    with filepath.open(encoding=ENCODING) as f:
        header = next(csv.reader(f))
    return header


def check_pairing():
    assert all(p.exists() for p in (LANG_META, LANG))
    mcols = list(yaml_load(LANG_META))
    header = csv_header(LANG)
    assert header == mcols

    assert [m.stem for m in METADATA] == [d.stem for d in DATA]
    for m, d in zip(METADATA, DATA):
        mcols = list(yaml_load(m))
        header = csv_header(d)
        assert header[0] == 'LID'
        assert header[1:] == mcols


def itermetadata(filepaths=METADATA):
    for p in filepaths:
        for v in load_metadata(p):
            yield v


def load_metadata(filepath):
    def itervariables(module, doc):
        for variable, v in doc.items():
            v['Module'] = module
            v['Variable'] = variable
            yield v

    module = filepath.stem
    doc = yaml_load(filepath)
    return list(itervariables(module, doc))


class LanguageTable(object):

    meta, data = LANG_META, LANG

    component = 'LanguageTable'

    primary_key = ['LID']

    columns = {
        'LID': {
            'property': 'id',
            'datatype': 'integer',
            'required': True,
        },
        'Language': {
            'property': 'name',
            'required': True,
        },
        'LanguageAlternativeNames': {
            'separator': ', ',
        },
        'Genesis': {
            'datatype': {'base': 'string', 'format': '|'.join(['regular', 'creole', 'mixed'])},
        },
        'Longitude': {
            'property': 'longitude',
            'datatype': {'base': 'decimal', 'minimum': -180, 'maximum': 180},
        },
        'Latitude': {
            'property': 'latitude',
            'datatype': {'base': 'decimal', 'minimum': -90, 'maximum': 90},
        },
        'NearProtoHomeland':  {
            'datatype': {'base': 'boolean', 'format': '|'.join(['TRUE', 'FALSE'])},
        },
        'Modality': {
            'datatype': {'base': 'string', 'format': '|'.join(['spoken', 'signed'])},
        },
        'LocalRegion': {
            'datatype': {'base': 'string', 'format': '|'.join(['Caucasus', 'Himalaya'])},
            'null': ['', 'NA'],
        },
        'StockAllNames': {
            'separator': ', ',
        },        
        'LanguageAllNames': {
            'separator': ', ',
        },        
        'ISO639.3': {
            'property': 'iso639P3code',
            'datatype': {'base': 'string', 'format': '[a-z]{3}'},
        },
        'Glottocode': {
            'property': 'glottocode',
            'datatype': {'base': 'string', 'format': '[a-z0-9]{4}[1-9][0-9]{3}'},
        },
        'Macrocontinent': {
            'datatype': {
                'base': 'string',
                'format': '|'.join(['Africa', 'Eurasia', ' Pacific', 'Americas']),
            },
            'null': ['NA'],
        },
    }

    @classmethod
    def as_component(cls):
        component = {
            'url': ('..' / cls.data).as_posix(),  # FIXME
            'dc:conformsTo': CLDF + cls.component,
            'tableSchema': {'primaryKey': cls.primary_key},
        }

        def itercolumns(meta, columns):
            for name, d in meta.items():
                col = {
                    'name': name,
                    'dc:description': d['Description'].strip(),
                }
                if d['N.missing'] == 0:
                    col['required'] = True
                if 'Levels' in d:
                    format_ = '|'.join(d['Levels'])
                    col['datatype'] = {'base': 'string', 'format': format_}
                if name in columns:
                    col.update(columns[name])
                    if 'property' in col:
                        col['propertyUrl'] = CLDF + col.pop('property')
                yield col

        meta = yaml_load(cls.meta)
        assert all(c in meta for c in cls.columns)

        columns = list(itercolumns(meta, cls.columns))
        add_component_args = [component] + columns
        return add_component_args


if __name__ == '__main__':
    #check_pairing()
    #df = pd.DataFrame(itermetadata()).set_index(METACOLS[:2])[METACOLS[2:]]
    d = pycldf.StructureDataset.in_dir(OUT_DIR, empty_tables=True)
    d.add_component(*LanguageTable.as_component())
    p = d.write_metadata()
    tg = csvw.TableGroup.from_file(p)
    t = tg.tables[0]
    pprint(next(t.iterdicts()))
