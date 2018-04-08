# Domesday

In 1085, William I of England commissioned an survey of all taxable lands in England.
Its purpose was to assess taxes owed under Edward the Confessor, and record the redistribution of land under the Norman aristocracy.

The survey, published in 1086, came to be known as Domesday Book ("Doom's Day") because its judgements were final.

The [Prosopography of Anglo-Saxon England (PASE)](http://pase.ac.uk/) offers a [searchable database of Domesday Book landholders](http://domesday.pase.ac.uk/).

This project lets you import the data into an SQLite3 database or [Pandas](https://pandas.pydata.org/) for further analysis.

## Requirements

* Python 3.6
* `wget` (for Jupyter Notebook)

## Install

```
pipenv install
```

## Usage

### Python

Search for landholders directly.

```python
import domesday

db = domesday.Database('domesday.db')
db.load_csv('domesday.csv')

with db.connection as conn:
    landholders = conn.execute('SELECT * FROM landholders')

print(landholders.fetchone())
('Edward', 'Male', 'Edward 15', 'Edward, king, fl. 1066', Decimal('8230.05'), Decimal('6924.10'), Decimal('0.00'), Decimal('0.00'), Decimal('0.00'), None, '2 of 5')
```

Search for landholders using [full-text search](https://sqlite.org/fts3.html).

```python
with db.connection as conn:
    landholders = conn.execute('SELECT * FROM fts_landholders WHERE fts_landholders MATCH "godric"')

print(landholders.fetchone())
('Godric', 'Godric 57', 'Godric, abbot of Winchcombe, fl. 1066')
```

### Jupyter Notebook and Pandas

You can also explore the database using Pandas.

Check out [domesday.ipyndb](domesday.ipynb) for an example.
