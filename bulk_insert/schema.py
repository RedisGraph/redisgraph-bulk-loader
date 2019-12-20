# Official enum support varies widely between 2.7 and 3.x, so we'll use a custom class
class Type:
    NULL = 0
    BOOL = 1
    DOUBLE = 2
    STRING = 3
    INTEGER = 4
    ID = 5
    LABEL = 6
    TYPE = 7
    START_ID = 8
    END_ID = 9
    IGNORE = 10


def convert_schema_type(in_type):
    return {
        'null': Type.NULL,
        'boolean': Type.BOOL,
        'double': Type.DOUBLE,
        'string': Type.STRING,
        'integer': Type.INTEGER,
        'id': Type.ID,
        'label': Type.LABEL,
        'type': Type.TYPE,
        'start_id': Type.START_ID,
        'end_id': Type.END_ID
        }[in_type]
