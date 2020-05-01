[![license](https://img.shields.io/github/license/RedisGraph/redisgraph-bulk-loader.svg)](https://github.com/RedisGraph/redisgraph-bulk-loader)
[![CircleCI](https://circleci.com/gh/RedisGraph/redisgraph-bulk-loader/tree/master.svg?style=svg)](https://circleci.com/gh/RedisGraph/redisgraph-bulk-loader/tree/master)
[![Release](https://img.shields.io/github/release/RedisGraph/redisgraph-bulk-loader.svg)](https://github.com/RedisGraph/redisgraph-bulk-loader/releases/latest)
[![PyPI version](https://badge.fury.io/py/redisgraph-bulk-loader.svg)](https://badge.fury.io/py/redisgraph-bulk-loader)
[![Codecov](https://codecov.io/gh/RedisGraph/redisgraph-bulk-loader/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisGraph/redisgraph-bulk-loader)

# redisgraph-bulk-loader
[![Forum](https://img.shields.io/badge/Forum-RedisGraph-blue)](https://forum.redislabs.com/c/modules/redisgraph)
[![Gitter](https://badges.gitter.im/RedisLabs/RedisGraph.svg)](https://gitter.im/RedisLabs/RedisGraph?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

A Python utility for building RedisGraph databases from CSV inputs

## Requirements
The bulk loader utility requires a Python 3 interpreter.

A Redis server with the [RedisGraph](https://github.com/RedisLabsModules/RedisGraph) module must be running. Installation instructions may be found at:
https://oss.redislabs.com/redisgraph/

## Installation
The bulk loader script's dependencies can be resolved using pip:
```
pip install git+https://github.com/RedisGraph/redisgraph-bulk-loader.git@master
```

## Usage
redisgraph-bulk-insert GRAPHNAME [OPTIONS]

| Flags   | Extended flags        |    Parameter                                                    |
|---------|-----------------------|-----------------------------------------------------------------|
|  -h     | --host TEXT           |    Redis server host (default: 127.0.0.1)                       |
|  -p     | --port INTEGER        |    Redis server port   (default: 6379)                          |
|  -a     | --password TEXT       |    Redis server password                                        |
|  -n     | --nodes TEXT          |    path to node csv file [required]                             |
|  -r     | --relations TEXT      |    path to relationship csv file                                |
|  -t     | --max-token-count INT |    max number of tokens sent in each Redis query (default 1024) |
|  -b     | --max-buffer-size INT |    max batch size (MBs) of each Redis query (default 4096)      |
|  -c     | --max-token-size INT  |    max size (MBs) of each token sent to Redis (default 500)     |
|  -q     | --quote               |    the quoting format used in the CSV file. QUOTE_MINIMAL=0,QUOTE_ALL=1,QUOTE_NONNUMERIC=2,QUOTE_NONE=3 |
|  -f     | --field-types         |    json to set explicit types for each field, format {<label>:[<col1 type>, <col2 type> ...]} where type can be 0(null),1(bool),2(numeric),3(string)       |


The only required arguments are the name to give the newly-created graph (which can appear anywhere) and at least one node CSV file.
The nodes and relationship flags should be specified once per input file.

The flags for `max-token-count`, `max-buffer-size`, and `max-token-size` should only be specified if the memory overhead of graph creation is too high. The bulk loader builds large graphs by sending binary tokens (each of which holds multiple nodes or relations) to Redis in batches. By lowering these limits from their defaults, the size of each transmission to Redis is lowered and fewer entities are held in memory, at the expense of a longer overall runtime.

```
redisgraph-bulk-insert GRAPH_DEMO -n example/Person.csv -n example/Country.csv -r example/KNOWS.csv -r example/VISITED.csv
```
The label (for nodes) or relationship type (for relationships) is derived from the base name of the input CSV file. In this example, we'll construct two sets of nodes, labeled `Person` and `Country`, and two types of relationships - `KNOWS` and `VISITED`.

The default behaviour is to infer the type for each row based on the value of each row, which can cause type mismatch problem. For example if a string property contains string values of 'false', 'true' or numbers. To avoid this, use --field-types to explicitly set the type for each column in the csv. 
EG, to explicitly set to string.

```
redisgraph-bulk-insert ROBOTS -f '{"Robots" : [3]}' -q1 -n example2/Robots.csv 
```

Notice that when -f isn't used, the robot name "30165" would be inserted as a number rather than a string which causes problems in RedisGraph when searching. 

## Input constraints
### Node identifiers
- If both nodes and relations are being created, each node must be associated with a unique identifier.
- The identifier is the first column of each label CSV file. If this column's name starts with an underscore (`_`), the identifier is internal to the bulk loader operation and does not appear in the resulting graph. Otherwise, it is treated as a node property.
- Each identifier must be entirely unique across all label files.
- Source and destination nodes in relation CSV files should be referred to by their identifiers.
- The uniqueness restriction is lifted if only nodes are being created.

### Entity properties
- Property types do not need to be explicitly provided.
- Properties are not required to be exclusively composed of any type.
- The types currently supported by the bulk loader are:
    - `boolean`: either `true` or `false` (case-insensitive, not quote-interpolated).
    - `numeric`: an unquoted value that can be read as a floating-point or integer type.
    - `string`: any field that is either quote-interpolated or cannot be casted to a numeric or boolean type.
    - `NULL`: an empty field.
- Default behaviour is to infer the property type, attempting to cast it to null, float, boolean or string in that order. 
- If explicit type is required, for example, if a value is "1234" and it must not be inferred into a float, you can use the option -f to specify the type explicitly for each row being imported. 

### Label file format:
- Each row must have the same number of fields.
- Leading and trailing whitespace is ignored.
- The first field of a label file will be the node identifier, as described in [Node Identifiers](#node-identifiers).
- All fields are property keys that will be associated with each node.

### Relationship files
- Each row must have the same number of fields.
- Leading and trailing whitespace is ignored.
- The first two fields of each row are the source and destination node identifiers. The names of these fields in the header do not matter.
- If the file has more than 2 fields, all subsequent fields are relationship properties that adhere to the same rules as node properties.
- Described relationships are always considered to be directed (source->destination).

