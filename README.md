# redisgraph-bulk-loader
A Python utility for building RedisGraph databases from CSV inputs

## Installation
The bulk loader script's dependencies can be resolved using pip:
```
pip install --user -r requirements.txt
```

A Redis server with the [RedisGraph](https://github.com/RedisLabsModules/RedisGraph) module must be running. Installation instructions may be found at:
https://oss.redislabs.com/redisgraph/

## Usage
bulk_insert.py GRAPHNAME [OPTIONS]

| Flags   | Extended flags        |    Parameter                                 |
|---------|-----------------------|----------------------------------------------|
|  -h     | --host TEXT           |    Redis server host (default: 127.0.0.1)    |
|  -p     | --port INTEGER        |    Redis server port   (default: 6379)       |
|  -P     | --password TEXT       |    Redis server password                     |
|  -s     | --ssl TEXT            |    Server is SSL-enabled                     |
|  -n     | --nodes TEXT          |    path to node csv file  [required]         |
|  -r     | --relationships TEXT  |    path to relationship csv file             |

The only required arguments are the name to give the newly-created graph (which can appear anywhere) and at least one node CSV file.
The nodes and relationship flags should be specified once per input file.

```
python bulk_insert.py GRAPH_DEMO -n demo/Person.csv -n demo/Country.csv -r demo/KNOWS.csv -r demo/VISITED.csv
```
The label (for nodes) or relationship type (for relationships) is derived from the base name of the input CSV file. In this query, we'll construct two sets of nodes, labeled `Person` and `Country`, and two types of relationships - `KNOWS` and `VISITED`.

## Input constraints
### Node files
- Node inputs are expected to be in a conventional table format. Each field in the header is a property name, which for each node is associated with the value in that column.
- Each row must have the same number of fields.
- Extraneous whitespace is ignored.
- Value types do not need to be provided. Properties are not required to be exclusively composed of numeric or string types.
- There is no uniqueness constraint on nodes.

### Relationship files
- Relationship inputs have no headers.
- Each row should specify a source and destination node ID.
- Described relationships are always considered to be directed (source->destination).
- The bulk insert script does not yet support adding properties to relationships (though this can be done after the fact with RedisGraph queries).
- _NOTE_ Relationship processing does not yet include node lookups. The entries in a relationship file should all be integers corresponding to node IDs.


### Determining Node IDs
Node IDs are assigned in order of insertion. Node files are processed in the order specified by the user on the command line (though all Node files are processed before Relationship files).

The first node in the first node file will have an ID of 0, and subsequent nodes across all files are ordered consecutively.

If a relationship file has the line:
```
0,11
```
This indicates that there is a relationship from the first node in the first node file to the 12th node to be inserted, regardless of which file it may appear in.

