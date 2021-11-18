[![license](https://img.shields.io/github/license/RedisGraph/redisgraph-bulk-loader.svg)](https://github.com/RedisGraph/redisgraph-bulk-loader)
[![CircleCI](https://circleci.com/gh/RedisGraph/redisgraph-bulk-loader/tree/master.svg?style=svg)](https://circleci.com/gh/RedisGraph/redisgraph-bulk-loader/tree/master)
[![Release](https://img.shields.io/github/release/RedisGraph/redisgraph-bulk-loader.svg)](https://github.com/RedisGraph/redisgraph-bulk-loader/releases/latest)
[![PyPI version](https://badge.fury.io/py/redisgraph-bulk-loader.svg)](https://badge.fury.io/py/redisgraph-bulk-loader)
[![Codecov](https://codecov.io/gh/RedisGraph/redisgraph-bulk-loader/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisGraph/redisgraph-bulk-loader)

# redisgraph-bulk-loader
[![Forum](https://img.shields.io/badge/Forum-RedisGraph-blue)](https://forum.redis.com/c/modules/redisgraph)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/gWBRT6P)

A Python utility for building RedisGraph databases from CSV inputs

## Requirements
The bulk loader utility requires a Python 3 interpreter.

A Redis server with the [RedisGraph](https://github.com/RedisLabsModules/RedisGraph) module must be running. Installation instructions may be found at:
https://oss.redis.com/redisgraph/

## Installation
The bulk loader can be installed using pip:
```
pip install redisgraph-bulk-loader
```
Or
```
pip install git+https://github.com/RedisGraph/redisgraph-bulk-loader.git@master
```

## Usage
Pip installation exposes `redisgraph-bulk-insert` as a command to invoke this tool:
```
redisgraph-bulk-insert GRAPHNAME [OPTIONS]
```

Installation by cloning the repository allows the script to be invoked via Python like so:
```
python3 redisgraph_bulk_loader/bulk_insert.py GRAPHNAME [OPTIONS]
```

| Flags | Extended flags             |                                              Parameter                                               |
|:-----:|----------------------------|:----------------------------------------------------------------------------------------------------:|
|  -h   | --host TEXT                |                                Redis server host (default: 127.0.0.1)                                |
|  -p   | --port INTEGER             |                                  Redis server port (default: 6379)                                   |
|  -a   | --password TEXT            |                                Redis server password (default: none)                                 |
|  -u   | --unix-socket-path TEXT    |                                Redis unix socket path (default: none)                                |
|  -k   | --ssl-keyfile TEXT         |                                 Path to SSL keyfile (default: none)                                  |
|  -l   | --ssl-certfile TEXT        |                                 Path to SSL certfile (default: none)                                 |
|  -m   | --ssl-ca-certs TEXT        |                                 Path to SSL CA certs (default: none)                                 |
|  -n   | --nodes TEXT               |                      Path to Node CSV file with the filename as the Node Label                       |
|  -N   | --nodes-with-label TEXT    |                             Node Label followed by path to Node CSV file                             |
|  -r   | --relations TEXT           |               Path to Relationship CSV file with the filename as the Relationship Type               |
|  -R   | --relations-with-type TEXT |                     Relationship Type followed by path to relationship CSV file                      |
|  -o   | --separator CHAR           |                         Field token separator in CSV files (default: comma)                          |
|  -d   | --enforce-schema           |                 Requires each cell to adhere to the schema defined in the CSV header                 |
|  -j   | --id-type TEXT             |                The data type of unique node ID properties (either STRING or INTEGER)                 |
|  -s   | --skip-invalid-nodes       |            Skip nodes that reuse previously defined IDs instead of exiting with an error             |
|  -e   | --skip-invalid-edges       |            Skip edges that use invalid IDs for endpoints instead of exiting with an error            |
|  -q   | --quote INT                | The quoting format used in the CSV file. QUOTE_MINIMAL=0,QUOTE_ALL=1,QUOTE_NONNUMERIC=2,QUOTE_NONE=3 |
|  -t   | --max-token-count INT      |            (Debug argument) Max number of tokens sent in each Redis query (default 1024)             |
|  -b   | --max-buffer-size INT      |                (Debug argument) Max batch size (MBs) of each Redis query (default 64)                |
|  -c   | --max-token-size INT       |               (Debug argument) Max size (MBs) of each token sent to Redis (default 64)               |
|  -i   | --index Label:Property     |              After bulk import, create an Index on provided Label:Property pair (optional)           |
|  -f   | --full-text-index Label:Property     |              After bulk import, create an full text index on provided Label:Property pair (optional)           |


The only required arguments are the name to give the newly-created graph (which can appear anywhere) and at least one node CSV file.
The nodes and relationship flags should be specified once per input file.

```
redisgraph-bulk-insert GRAPH_DEMO -n example/Person.csv -n example/Country.csv -r example/KNOWS.csv -r example/VISITED.csv
```
The label (for nodes) or relationship type (for relationships) is derived from the base name of the input CSV file. In this example, we'll construct two sets of nodes, labeled `Person` and `Country`, and two types of relationships - `KNOWS` and `VISITED`.

RedisGraph does not impose a schema on properties, so the same property key can have values of differing types, such as strings and integers. As such, the bulk loader's default behaviour is to infer the type for each field independently for each value. This can cause unexpected behaviors when, for example, a property expected to always have string values has a field that can be cast to an integer or double. To avoid this, use the `--enforce-schema` flag and update your CSV headers as described in [Input Schemas](#input-schemas).

### Extended parameter descriptions
The flags for `max-token-count`, `max-buffer-size`, and `max-token-size` are typically not required. They should only be specified if the memory overhead of graph creation is too high, or raised if the volume of Redis calls is too high. The bulk loader builds large graphs by sending binary tokens (each of which holds multiple nodes or relations) to Redis in batches.

`--quote` is maintained for backwards compatibility, and allows some control over Python's type inference in the default mode. `--enforce-schema-type` is preferred.

`--enforce-schema-type` indicates that input CSV headers will follow the form described in [Input Schemas](#input-schemas).

`--nodes-with-label` and `--relations-with-type` allows the node label or relationship type to be explicitly written instead of inferring them from the filename. For example, `--relations-with-type HAS_TAG post_hasTag_tag.csv` will add all relationships described in the specified CSV with the type `HAS_TAG`.

## Input constraints
### Node identifiers
- If both nodes and relations are being created, each node must be associated with a unique identifier.
- If not using `--enforce-schema`, the identifier is the first column of each label CSV file. If this column's name starts with an underscore (`_`), the identifier is internal to the bulk loader operation and does not appear in the resulting graph. Otherwise, it is treated as a node property.
- Each identifier must be entirely unique across all label files. [ID namespaces](#id-namespaces) can be used to write more granular identifiers.
- Source and destination nodes in relation CSV files should be referred to by their identifiers.
- The uniqueness restriction is lifted if only nodes are being created.

### Entity properties
- Property types do not need to be explicitly provided.
- Properties are not required to be exclusively composed of any type.
- The types currently supported by the bulk loader are:
    - `bool`: either `true` or `false` (case-insensitive, not quote-interpolated).
    - `integer`: an unquoted value that can be read as an integer type.
    - `double`: an unquoted value that can be read as a floating-point type.
    - `string`: any field that is either quote-interpolated or cannot be casted to a numeric or boolean type.
    - `array`: A bracket-interpolated array of elements of any types. Strings within the array must be explicitly quote-interpolated. Array properties require use of a non-comma delimiter for the CSV (`-o`).
- Cypher does not allow NULL values to be assigned to properties.
- The default behaviour is to infer the property type, attempting to cast it to integer, float, boolean, or string in that order.
- The `--enforce-schema` flag and an [Input Schema](#input-schemas) should be used if type inference is not desired.

### Label file format:
- Each row must have the same number of fields.
- Leading and trailing whitespace is ignored.
- If not using an [Input Schema](#input-schemas), the first field of a label file will be the node identifier, as described in [Node Identifiers](#node-identifiers).
- All fields are property keys that will be associated with each node.

### Relationship files
- Each row must have the same number of fields.
- Leading and trailing whitespace is ignored.
- If not using an [Input Schema](#input-schemas), the first two fields of each row are the source and destination node identifiers. The names of these fields in the header do not matter.
- If the file has more than 2 fields, all subsequent fields are relationship properties that adhere to the same rules as node properties.
- Described relationships are always considered to be directed (source->destination).

### Input CSV example
Store.csv
```
storeNum | Location | daysOpen |
118 | 123 Main St | ['Mon', 'Wed', 'Fri']
136 | 55 Elm St | ['Sat', 'Sun']
```
This CSV would be inserted with the command:
`redisgraph-bulk-insert StoreGraph --separator \| --nodes Store.csv`

(Since the pipe character has meaning in the terminal, it must be backslash-escaped.)

All `storeNum` properties will be inserted as integers, `Location` will be inserted as strings, and `daysOpen` will be inserted as arrays of strings.

## Input Schemas
If the `--enforce-schema` flag is specified, all input CSVs will be expected to specify each column's data type in the header.

This format lifts some constraints of the default CSV format, such as ID fields being the first column.

Most header fields should be a colon-separated pair of the property name and its data type, such as `Name:STRING`. Certain data types do not require a name string, as indicated below.

The accepted data types are:
|     Type String      | Description                                                       | Requires name string |
|:--------------------:|-------------------------------------------------------------------|:--------------------:|
|          ID          | Label files only - Unique identifier for a node                   |       Optional       |
|       START_ID       | Relation files only - The ID field of this relation's source      |          No          |
|        END_ID        | Relation files only - The ID field of this relation's destination |          No          |
|        IGNORE        | This column will not be added to the graph                        |       Optional       |
|    DOUBLE / FLOAT    | A signed 64-bit floating-point value                              |         Yes          |
| INT / INTEGER / LONG | A signed 64-bit integer value                                     |         Yes          |
|    BOOL / BOOLEAN    | A boolean value indicated by the string 'true' or 'false'         |         Yes          |
|        STRING        | A string value                                                    |         Yes          |
|        ARRAY         | An array value                                                    |         Yes          |

If an `ID` column has a name string, the value will be added to each node as a property. This property will be a string by default, though it may be switched to integer using the `--id-type` argument. If the name string is not provided, the ID is internal to the bulk loader operation and will not appear in the graph. `START_ID` and `END_ID` columns will never be added as properties.

### ID Namespaces
Typically, node identifiers need to be unique across all input CSVs. When using an input schema, it is (optionally) possible to create ID namespaces, and the identifier only needs to be unique across its namespace. This is particularly useful when each input CSV has primary keys which overlap with others.

To introduce a namespace, follow the `:ID` type string with a parentheses-interpolated namespace string, such as `:ID(User)`. The same namespace should be specified in the `:START_ID` or `:END_ID` field of relation files, as in `:START_ID(User)`.

### Input Schema CSV examples
User.csv
```
:ID(User), name:STRING, rank:INT
0, "Jeffrey", 5
1, "Filipe", 8
```

FOLLOWS.csv
```
:START_ID(User), :END_ID(User), reaction_count:INT
0, 1, 25
1, 0, 10
```
Inserting these CSVs with the command:
`redisgraph-bulk-insert SocialGraph --enforce-schema --nodes User.csv --relations FOLLOWS.csv`

Will produce a graph named SocialGraph with 2 users, Jeffrey and Filipe. Jeffrey follows Filipe, and that relation has a reaction_count of 25. Filipe also follows Jeffrey, with a reaction_count of 10.

## Performing bulk updates
Pip installation also exposes the command `redisgraph-bulk-update`:
```
redisgraph-bulk-update GRAPHNAME [OPTIONS]
```

Installation by cloning the repository allows the bulk updater to be invoked via Python like so:
```
python3 redisgraph_bulk_loader/bulk_update.py GRAPHNAME [OPTIONS]
```

| Flags | Extended flags           |                         Parameter                          |
|:-----:|--------------------------|:----------------------------------------------------------:|
|  -h   | --host TEXT              |           Redis server host (default: 127.0.0.1)           |
|  -p   | --port INTEGER           |             Redis server port (default: 6379)              |
|  -a   | --password TEXT          |           Redis server password (default: none)            |
|  -u   | --unix-socket-path TEXT  |           Redis unix socket path (default: none)           |
|  -q   | --query TEXT             |                   Query to run on server                   |
|  -v   | --variable-name TEXT     |   Variable name for row array in queries (default: row)    |
|  -c   | --csv TEXT               |                   Path to CSV input file                   |
|  -o   | --separator TEXT         |             Field token separator in CSV file              |
|  -n   | --no-header              |             If set, the CSV file has no header             |
|  -t   | --max-token-size INTEGER | Max size of each token in megabytes (default 500, max 512) |

The bulk updater allows a CSV file to be read in batches and committed to RedisGraph according to the provided query.

For example, given the CSV files described in [Input Schema CSV examples](#input-schema-csv-examples), the bulk loader could create the same nodes and relationships with the commands:
```
redisgraph-bulk-update SocialGraph --csv User.csv --query "MERGE (:User {id: row[0], name: row[1], rank: row[2]})"
redisgraph-bulk-update SocialGraph --csv FOLLOWS.csv --query "MATCH (start {id: row[0]}), (end {id: row[1]}) MERGE (start)-[f:FOLLOWS]->(end) SET f.reaction_count = row[2]"
```

When using the bulk updater, it is essential to sanitize CSV inputs beforehand, as RedisGraph *will* commit changes to the graph incrementally. As such, malformed inputs may leave the graph in a partially-updated state.
