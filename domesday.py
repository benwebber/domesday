import argparse
import csv
from decimal import Decimal
import operator
from pathlib import Path
import sqlite3
import sys
from typing import (
    Any,
    List,
    NamedTuple,
    Optional,
    TextIO,
    Union,
    TypeVar,
)


Filename = Union[str, Path]
CSVFile = Union[Filename, TextIO]
ExitCode = Optional[int]
Value = TypeVar('Value')


try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import sqlalchemy
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False


class Landholder(NamedTuple):
    """
    A Landholder in the PASE Domesday database.

    Decimal fields represent the total taxable value of estates held in whole or
    part by this person in hides.
    """
    name: Optional[str]
    gender: Optional[str]
    pase_name: str
    description: str
    holder_1066: Decimal
    lord_1066: Decimal
    demesne_1086: Decimal
    subtenanted_1086: Decimal
    subtenant_1086: Decimal
    editor: Optional[str]
    editorial_status: str

    @classmethod
    def from_row(cls, row: List[str]) -> 'Landholder':
        """
        Return a Landholder from a CSV or database row.
        """
        return cls._make(row).clean()

    def clean(self) -> 'Landholder':
        """
        Clean field values and cast values to declared types.
        """
        cleaned_data = self._asdict()
        for field, typ in self._field_types.items():
            # Clean field if cleaner exists.
            try:
                cleaned_data[field] = operator.methodcaller(f'clean_{field}')(self)
            except AttributeError:
                pass
            # Cast field to declared type.
            try:
                value = cleaned_data[field]
                cleaned_data[field] = typ(value)
            except TypeError:
                pass
        return self._replace(**cleaned_data)

    def clean_description(self) -> str:
        """
        Trim the description field and convert typographic quotation marks to
        vertical quotation marks.
        """
        return (self.description.strip(' "')
                                .replace('‘', "'")
                                .replace('’', "'"))

    def clean_editor(self) -> Optional[str]:
        ""
        return _clean_null(self.editor)

    def clean_gender(self) -> Optional[str]:
        return _clean_null(self.gender)

    def clean_name(self) -> Optional[str]:
        return _clean_null(self.name)

    def clean_pase_name(self) -> str:
        """
        Remove excess spaces in PASE name.
        """
        return ' '.join(self.pase_name.split())


class Database:
    SCHEMA = '''
CREATE TABLE IF NOT EXISTS landholders (
    name              TEXT,
    gender            TEXT,
    pase_name         TEXT NOT NULL PRIMARY KEY,
    description       TEXT NOT NULL,
    -- TEXT_DECIMAL type matches TEXT affinity, so SQLite will not strip
    -- leading or trailing zeros.
    holder_1066       TEXT_DECIMAL NOT NULL,
    lord_1066         TEXT_DECIMAL NOT NULL,
    demesne_1086      TEXT_DECIMAL NOT NULL,
    subtenanted_1086  TEXT_DECIMAL NOT NULL,
    subtenant_1086    TEXT_DECIMAL NOT NULL,
    editor            TEXT,
    editorial_status  TEXT NOT NULL,
                      UNIQUE (pase_name) ON CONFLICT REPLACE
);

CREATE VIRTUAL TABLE IF NOT EXISTS fts_landholders USING fts4 (
    content="landholders",
    name,
    pase_name,
    description,
);

CREATE INDEX IF NOT EXISTS idx_landholders_gender ON landholders(gender);
'''

    def __init__(self, database: Filename) -> None:
        if isinstance(database, Path):
            database = str(database)
        self._database = database
        self.connection = sqlite3.connect(self._database, detect_types=sqlite3.PARSE_DECLTYPES)
        self._create_schema()

    def _create_schema(self) -> sqlite3.Cursor:
        with self.connection as conn:
            return conn.executescript(self.SCHEMA)

    def load_csv(self, csv_file: CSVFile) -> None:
        """
        Import records from the PASE Domesday CSV database.
        """
        if isinstance(csv_file, str):
            csv_file = Path(csv_file)
        if isinstance(csv_file, Path):
            csv_file = open(csv_file)
        reader = csv.reader(csv_file)
        with self.connection as conn:
            conn.executemany(
                'INSERT INTO landholders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (parse_row(row) for row in reader)
            )
            conn.execute("INSERT INTO fts_landholders(fts_landholders) VALUES ('rebuild')")

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert the database to a Pandas dataframe.
        """
        if not HAS_PANDAS and HAS_SQLALCHEMY:
            raise Exception
        engine = sqlalchemy.create_engine(f'sqlite:///{self._database}')
        df = pd.read_sql_table('landholders', engine)
        # Convert holding columns from objects (strings) to numerics.
        holdings = [field for field, typ in Landholder._field_types.items()
                    if typ is Decimal]
        df[holdings] = df[holdings].apply(pd.to_numeric)
        return df


def parse_row(row: List[str]) -> Landholder:
    if len(row) >= len(Landholder._fields):
        # One of the fields is malformed. The description field probably
        # contains a misplaced comma (Thorkil 92 and Wulfric 280).
        description_components = row[3:-7]
        description = ','.join(description_components)
        row = row[:3] + [description] + row[-7:]
    assert len(row) == len(Landholder._fields)
    return Landholder.from_row(row)


def _clean_null(value: Value) -> Optional[Value]:
    """
    Convert pseudo-null values to None (SQL NULL).
    """
    if value is None:
        return None
    if isinstance(value, str) and value.lower() in {'', 'null', 'undefined'}:
        return None
    return value


def parse_args(argv: Optional[List[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('csv', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('database', nargs='?')
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> ExitCode:
    rc = 0
    args = parse_args(argv)
    db = Database(args.database)
    db.load_csv(args.csv)
    return rc


sqlite3.register_adapter(Decimal, lambda value: str(value))
sqlite3.register_converter('TEXT_DECIMAL', lambda value: Decimal(value.decode()))


if __name__ == '__main__':
    sys.exit(main())
