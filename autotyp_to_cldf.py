#!/usr/bin/env python3
# autotyp_to_cldf.py

import csv
import pathlib
import itertools
import contextlib
from pprint import pprint

import yaml

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
        doc = yaml.safe_load(f)  # NOTE: depends on mappings returned in order
    return doc


@contextlib.contextmanager
def csv_open(filepath, mode='r', encoding=ENCODING, dialect='excel'):
    result_cls = {'r': csv.reader, 'w': csv.writer}[mode]
    with filepath.open(mode, encoding=encoding, newline='') as f:
        yield result_cls(f)


def csv_header(filepath, encoding=ENCODING, dialect='excel'):
    with csv_open(filepath, encoding=encoding, dialect=dialect) as reader:
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
        component = {'url': url, 'dc:conformsTo': CLDF + self.component}
        add_component_args = [component] + self.columns()
        return add_component_args


class LanguageTable(Component):

    component = 'LanguageTable'

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
            #'property': 'iso639P3code',
            # FIXME: ['tokh', 'mixe', 'berb', 'cuic', 'esme', 'fris', 'sorb', 'chag']
            'datatype': {'base': 'string', 'format': '[a-z]{3,4}'},
            'null': ['', 'NA'],
        },
        'Glottocode': {
            #'property': 'glottocode',
            # FIXME: 'jin1260'
            'datatype': {'base': 'string', 'format': '[a-z0-9]{3,4}[1-9][0-9]{3}'},
        },
        'Macrocontinent': {
            'datatype': {
                'base': 'string',
                'format': '|'.join(['Africa', 'Eurasia', 'Pacific', 'Americas']),
            },
            'null': ['', 'NA'],
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
        path = out_dir / 'languages.csv'
        with csv_open(self.data) as reader, csv_open(path, 'w') as writer:
            writer.writerows(reader)
        return path.name


class ParameterTable(Component):

    component = 'ParameterTable'

    _columns = {
        'ID': {'property': 'id', 'datatype': 'integer', 'required': True},
        'Module': {'required': True},
        'Variable': {'property': 'name', 'required': True},
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
        with csv_open(path, 'w') as writer:
            cols = self._columns.keys()  # NOTE: depends on py 3.6 dict order
            writer.writerow(cols)
            pa_ids = itertools.count(1)
            for m in self.metadata:
                module = m.stem
                meta = yaml_load(m)
                for variable, v in meta.items():
                    cols = (v[c] for c in cols if c in v)
                    cols = [c.strip() if isinstance(c, str) else c for c in cols]
                    row = [next(pa_ids), module, variable] + cols
                    writer.writerow(row)
        return path.name


class CodeTable(Component):

    component = 'CodeTable'

    _columns = {
        'ID': {'property': 'id', 'datatype': 'integer', 'required': True},
        'Variable': {'property': 'parameterReference', 'datatype': 'integer', 'required': True},
        'Level': {'property': 'name', 'required': True},
        # TODO: separator?
        'Description': {'property': 'description', 'required': True},
    }

    def write(self, out_dir):
        path = out_dir / 'codes.csv'
        with csv_open(path, 'w') as writer:
            cols = self._columns.keys()
            writer.writerow(cols)
            co_ids, pa_ids = (itertools.count(1) for _ in range(2))
            for m in self.metadata:
                module = m.stem
                meta = yaml_load(m)
                for variable, v in meta.items():
                    pa_id = next(pa_ids)
                    for level, desc in v.get('Levels', {}).items():
                        row = [next(co_ids), pa_id, level, desc.strip() or level]
                        writer.writerow(row)
        return path.name


class ValueTable(Component):

    component = 'ValueTable'

    _columns = {
        'ID': {'property': 'id', 'datatype': 'integer', 'required': True},
        'Variable_ID': {'property': 'parameterReference', 'datatype': 'integer', 'required': True},
        # FIXME: 2915, 3000 missing
        'LID': {'property': 'languageReference', 'datatype': 'integer', 'required': True},
        'Value': {'property': 'value'},
        'Level_ID': {'property': 'codeReference', 'datatype': 'integer'},
    }

    def write(self, out_dir):
        path = out_dir / 'values.csv'
        with csv_open(path, 'w') as writer:
            cols = self._columns.keys()
            writer.writerow(cols)
            va_ids, co_ids, pa_ids = (itertools.count(1) for _ in range(3))
            for m, d in zip(self.metadata, self.data):
                meta = yaml_load(m)
                with csv_open(d) as reader:
                    cols = zip(*reader)
                    lids = next(cols)
                    assert lids[0] == 'LID'
                    values = list(cols)
                    assert len(values) == len(meta)
                for (variable, v), var_col  in zip(meta.items(), values):
                    pa_id = next(pa_ids)
                    assert var_col[0] == variable
                    pairs = zip(lids[1:], var_col[1:])
                    if 'Levels' in v:
                        levels = {'': None}
                        levels.update((l, next(co_ids)) for l in v['Levels'])
                        rows = ((next(va_ids), pa_id, lid, None, levels[var_value])
                                 for lid, var_value in pairs)
                    else:
                        rows = ((next(va_ids), pa_id, lid, var_value, None)
                                 for lid, var_value in pairs)
                    writer.writerows(rows)
        return path.name


if __name__ == '__main__':
    #check_pairing()
    #import pandas as pd
    #df = pd.DataFrame(itermetadata()).set_index(METACOLS[:2])[METACOLS[2:]]
    out_dir = OUT_DIR
    if not out_dir.exists():
        out_dir.mkdir()
    d = pycldf.StructureDataset.in_dir(out_dir, empty_tables=True)
    d.add_component(*LanguageTable().as_component(out_dir))
    d.add_component(*ParameterTable(metadata=METADATA).as_component(out_dir))
    d.add_component(*CodeTable(metadata=METADATA).as_component(out_dir))
    d.add_component(*ValueTable(metadata=METADATA, data=DATA).as_component(out_dir))
    print(d)
    pprint(next(d['LanguageTable'].iterdicts()))
    pprint(next(d['ParameterTable'].iterdicts()))
    pprint(next(d['CodeTable'].iterdicts()))
    pprint(next(d['ValueTable'].iterdicts()))
    d.validate()  # FIXME: fails (see above)
    d.stats()
