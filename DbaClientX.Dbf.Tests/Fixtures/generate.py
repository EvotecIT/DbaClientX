"""Independent fixture producer. Test-only: pip install dbf==0.99.11 dbfread==2.0.7.

Generated fictional records are MIT-licensed under this repository's license.
Run manually; neither Python nor these packages is needed to build or use the codec.
"""
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path

import dbf
from dbfread import DBF

root = Path(__file__).resolve().parent


def produce(name, profile, fields, rows, deleted):
    path = root / (name + '.dbf')
    table = dbf.Table(str(path), fields, dbf_type=profile, codepage='cp1252')
    table.open(mode=dbf.READ_WRITE)
    for row in rows:
        table.append(dict(zip(table.field_names, row)))
    dbf.delete(table[deleted])
    table.close()
    source = DBF(str(path), load=True, char_decode_errors='strict')
    return {'profile': profile, 'fields': fields,
            'input_rows': [[None if value is dbf.Null else value for value in row] for row in rows],
            'oracle_records': list(source), 'oracle_deleted_records': list(source.deleted)}


mixed = [
    ('Café £', Decimal('1234.50'), True, date(2020, 2, 29), 'First memo\r\nSecond line: naïve'),
    ('deleted', Decimal('-2.75'), False, date(2021, 1, 2), 'Deleted memo'),
    ('', None, None, None, ''),
]
tables = {}
for profile in ('db3', 'fp'):
    tables[profile] = produce(profile, profile,
        'NAME C(24); AMOUNT N(12,2); ACTIVE L; STAMP D; NOTES M', mixed, 1)
tables['vfp'] = produce('vfp', 'vfp',
    'NAME C(24) NULL; COUNT I; REAL B; PRICE Y; STAMP T NULL; NOTES M; RAW C(8) BINARY; BINMEMO M BINARY',
    [('Café', 42, 1.25, Decimal('-123.4567'), datetime(2020, 2, 29, 12, 34, 56, 789000), 'VFP memo', b'\x00\xff\x01 ABC ', b'\x00\xff\x01'),
     ('deleted', -17, 3.5, Decimal('1.0001'), datetime(2021, 1, 2), 'Deleted memo', b'12345678', b'\x02\x03'),
     (dbf.Null, 0, 0.0, Decimal('0'), dbf.Null, '', b'\x00' * 8, b'')], 1)


def encode(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return {'hex': value.hex()}
    raise TypeError(type(value).__name__)


manifest = {'producer': 'dbf 0.99.11 (BSD)', 'oracle': 'dbfread 2.0.7 (MIT)',
            'qualification': 'Python-produced fixtures, independently decoded; no native dBASE/FoxPro application qualification',
            'oracle_limitations': 'dbfread does not interpret VFP nullable or binary-character flags completely; original producer inputs qualify those fields',
            'tables': tables,
            'sha256': {p.name: sha256(p.read_bytes()).hexdigest()
                       for p in sorted(root.iterdir()) if p.suffix in ('.dbf', '.dbt', '.fpt')}}
(root / 'manifest.json').write_text(json.dumps(manifest, default=encode, indent=2) + '\n', encoding='utf-8')
print(json.dumps(manifest, default=encode, indent=2))
