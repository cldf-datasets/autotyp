#!/usr/bin/env python3
# autotyp_to_cldf.py

import csv
import pathlib
import itertools
import contextlib
from pprint import pprint

import yaml
import pandas as pd

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


@contextlib.contextmanager
def csv_open(filepath, mode='r', encoding=ENCODING, dialect='excel'):
    result_cls = {'r': csv.reader, 'w': csv.writer}[mode]
    with filepath.open(mode, encoding=encoding, newline='') as f:
        yield result_cls(f)


def csv_header(filepath, encoding=ENCODING, dialect='excel'):
    with csv.open(filepath, encoding=encoding, dialect=dialect) as reader:
        header = next(reader)
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


class Component(object):

    def __init__(self, metadata=None, data=None):
        assert any(a is not None for a in (metadata, data))
        self.metadata = metadata
        self.data = data

    def columns(self):
        def itercolumns(columns):
            for name, col in columns.items():
                col['name'] = name
                if 'property' in col:
                    col['propertyUrl'] = CLDF + col.pop('property')
                yield col

        return list(itercolumns(self._columns))

    def as_component(self, out_dir=None):
        url = self.write(out_dir)
        if url is None:
            return None
        component = {
            'url': url,
            'dc:conformsTo': CLDF + self.component,
            'tableSchema': {'primaryKey': self.primary_key},
        }
        columns = self.columns()
        add_component_args = [component] + columns
        return add_component_args


class LanguageTable(Component):

    component = 'LanguageTable'

    primary_key = ['LID']

    _columns = {
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

    def __init__(self, metadata=LANG_META, data=LANG):
        super(LanguageTable, self).__init__(metadata, data)

    def columns(self):
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

        meta = yaml_load(self.metadata)
        assert all(c in meta for c in self._columns)
        return list(itercolumns(meta, self._columns))

    def write(self, out_dir):
        assert out_dir is None
        return ('../..' / self.data).as_posix()  # FIXME


class ParameterTable(Component):

    component = 'ParameterTable'

    primary_key = ['Variable']

    _columns = {
        # skip: 'Module'
        'Variable': {'property': 'id', 'required': True},  # FIXME: replace dot?
        'SetUp': {
            'datatype': {
                'base': 'string',
                'format': '|'.join([
                    'single entry per language',
                    'single aggregated entry per language',
                    'multiple entries per language',
                ]),
            },
            'required': True,
        },
        'DataEntry': {
            'datatype': {'base': 'string', 'format': '|'.join(['by hand', 'derived'])},
            'required': True,
        },
        'VariableType': {
            'datatype': {
                'base': 'string',
                'format': '|'.join(['data', 'condition', 'register', 'details', 'quality']),
            },
            'required': True,
        },
        'DataType': {
            'datatype': {
                'base': 'string',
                'format': '|'.join(['logical', 'categorical', 'ratio', 'count']),
            },
            'required': True,
        },
        'N.levels': {'datatype': 'integer', 'required': True},
        'N.entries': {'datatype': 'integer', 'required': True},
        'N.languages': {'datatype': 'integer', 'required': True},
        'N.missing': {'datatype': 'integer', 'required': True},
        # skip: 'Levels'
        'Description': {'property': 'description', 'required': True},
        'Notes': {'property': 'comment'},
    }

    def write(self, out_dir):
        path = out_dir / 'parameters.csv'
        meta = yaml_load(self.metadata)
        with csv_open(path, 'w') as writer:
            cols = self._columns.keys()  # FIXME: py 3.6 dict order dependent
            writer.writerow(cols)
            for variable, v in meta.items():
                row = [variable] + [v[c] for c in cols  if c in v]
                row = [c.strip() if isinstance(c, str) else c for c in row]
                writer.writerow(row)
        return path.name


class CodeTable(Component):

    component = 'CodeTable'

    primary_key = ['Level']

    _columns = {
        # NOTE: composite foreign keys via virtual columns do not mix with id urls?
        'ID': {'property': 'id', 'datatype': 'integer', 'required': True},
        'Variable': {'property': 'parameterReference', 'required': True},
        'Level': {'property': 'name', 'required': True},
        # TODO: separator?
        'Description': {'property': 'description', 'required': True},
    }

    def write(self, out_dir):
        meta = yaml_load(self.metadata)
        if not any('Levels' in v for v in meta.values()):
            return None
        path = out_dir / 'codes.csv'
        iterids = itertools.count(1)
        with csv_open(path, 'w') as writer:
            cols = self._columns.keys()
            writer.writerow(cols)
            for variable, v in meta.items():
                for level, desc in v.get('Levels', {}).items():
                    row = [next(iterids), variable, level, desc.strip() or level]
                    writer.writerow(row)
        return path.name


if __name__ == '__main__':
    #check_pairing()
    #df = pd.DataFrame(itermetadata()).set_index(METACOLS[:2])[METACOLS[2:]]
    for m, d in zip(METADATA, DATA):
        out_dir = OUT_DIR / m.stem
        if not out_dir.exists():
            out_dir.mkdir()
        d = pycldf.StructureDataset.in_dir(out_dir, empty_tables=True)
        d.add_component(*LanguageTable().as_component())
        d.add_component(*ParameterTable(metadata=m).as_component(out_dir))
        c = CodeTable(metadata=m).as_component(out_dir)
        if c is not None:
            d.add_component(*c)
        d.write_metadata()
        print(d)
        pprint(next(d['LanguageTable'].iterdicts()))
        pprint(next(d['ParameterTable'].iterdicts()))
        if c is not None:
            pprint(next(d['CodeTable'].iterdicts()))
        print()
        break
