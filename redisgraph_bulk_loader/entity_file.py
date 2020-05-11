import os
import io
import csv
import math
import struct
import configs
from exceptions import CSVError, SchemaError


class Type:
    NULL = 0
    BOOL = 1
    DOUBLE = 2
    STRING = 3
    LONG = 4
    ID = 5
    START_ID = 8
    END_ID = 9
    IGNORE = 10


def convert_schema_type(in_type):
    try:
        return {
                'null': Type.NULL,
                'boolean': Type.BOOL,
                'double': Type.DOUBLE,
                'string': Type.STRING,
                'string[]': Type.STRING, # TODO tmp
                'integer': Type.LONG,
                'int': Type.LONG,
                'long': Type.LONG,
                'id': Type.ID,
                'start_id': Type.START_ID,
                'end_id': Type.END_ID
                }[in_type]
    except KeyError:
        # TODO tmp
        if in_type.startswith('id('):
            return Type.ID
        elif in_type.startswith('start_id('):
            return Type.START_ID
        elif in_type.startswith('end_id('):
            return Type.END_ID
        else:
            raise SchemaError("Encountered invalid field type '%s'" % in_type)


# Convert a single CSV property field into a binary stream.
# Supported property types are string, numeric, boolean, and NULL.
# type is either Type.DOUBLE, Type.BOOL or Type.STRING, and explicitly sets the value to this type if possible
def prop_to_binary(prop_val, prop_type):
    # All format strings start with an unsigned char to represent our prop_type enum
    format_str = "=B"
    if prop_val is None:
        # An empty field indicates a NULL property
        return struct.pack(format_str, Type.NULL)

    # If field can be cast to a float, allow it
    if prop_type is None or prop_type == Type.DOUBLE:
        try:
            numeric_prop = float(prop_val)
            if not math.isnan(numeric_prop) and not math.isinf(numeric_prop): # Don't accept non-finite values.
                return struct.pack(format_str + "d", Type.DOUBLE, numeric_prop)
        except:
            raise SchemaError("Could not parse '%s' as a double" % prop_val)

    # TODO add support for non-integer ID types
    if prop_type is None or prop_type == Type.LONG or prop_type == Type.ID:
        try:
            numeric_prop = int(float(prop_val))
            return struct.pack(format_str + "q", Type.LONG, numeric_prop)
        except:
            raise SchemaError("Could not parse '%s' as a long" % prop_val)

    if prop_type is None or prop_type == Type.BOOL:
        # If field is 'false' or 'true', it is a boolean
        if prop_val.lower() == 'false':
            return struct.pack(format_str + '?', Type.BOOL, False)
        elif prop_val.lower() == 'true':
            return struct.pack(format_str + '?', Type.BOOL, True)

    if prop_type is None or prop_type == Type.STRING:
        # If we've reached this point, the property is a string
        encoded_str = str.encode(prop_val) # struct.pack requires bytes objects as arguments
        # Encoding len+1 adds a null terminator to the string
        format_str += "%ds" % (len(encoded_str) + 1)
        return struct.pack(format_str, Type.STRING, encoded_str)

    # If it hasn't returned by this point, it is trying to set it to a type that it can't adopt
    raise Exception("unable to parse [" + prop_val + "] with type ["+repr(prop_type)+"]")


# Superclass for label and relation CSV files
class EntityFile(object):
    def __init__(self, filename, label):
        # The label or relation type string is the basename of the file
        if label:
            self.entity_str = label
        else:
            self.entity_str = os.path.splitext(os.path.basename(filename))[0]
        # Input file handling
        self.infile = io.open(filename, 'rt')

        # Initialize CSV reader that ignores leading whitespace in each field
        # and does not modify input quote characters
        self.reader = csv.reader(self.infile, delimiter=configs.separator, skipinitialspace=True, quoting=configs.quoting)

        self.packed_header = b''
        self.binary_entities = []
        self.binary_size = 0 # size of binary token

        self.convert_header() # Extract data from header row.
        self.count_entities() # Count number of entities/row in file.

    # Count number of rows in file.
    def count_entities(self):
        self.entities_count = 0
        self.entities_count = sum(1 for line in self.infile)
        # seek back
        self.infile.seek(0)
        return self.entities_count

    # Simple input validations for each row of a CSV file
    def validate_row(self, row):
        # Each row should have the same number of fields
        if len(row) != self.column_count:
            raise CSVError("%s:%d Expected %d columns, encountered %d ('%s')"
                           % (self.infile.name, self.reader.line_num, self.column_count, len(row), configs.separator.join(row)))

    # If part of a CSV file was sent to Redis, delete the processed entities and update the binary size
    def reset_partial_binary(self):
        self.binary_entities = []
        self.binary_size = len(self.packed_header)

    # Convert property keys from a CSV file header into a binary string
    def pack_header(self):
        # String format
        entity_bytes = self.entity_str.encode()
        fmt = "=%dsI" % (len(entity_bytes) + 1) # Unaligned native, entity name, count of properties
        args = [entity_bytes, self.prop_count]
        for idx in range(self.column_count):
            if self.skip_offsets[idx]:
                continue
            prop = self.column_names[idx].encode()
            fmt += "%ds" % (len(prop) + 1) # encode string with a null terminator
            args.append(prop)
        return struct.pack(fmt, *args)

    # Extract column names and types from a header row
    def convert_header(self):
        header = next(self.reader)
        self.column_count = len(header)
        self.column_names = [None] * self.column_count   # Property names of every column.
        self.types = [None] * self.column_count          # Value type of every column.
        self.skip_offsets = [False] * self.column_count  # Whether column at any offset should not be stored as a property.

        for idx, field in enumerate(header):
            pair = field.split(':')
            if len(pair) > 2:
                raise CSVError("Field '%s' had %d colons" % field, len(field))

            if len(pair[0]) == 0: # Delete empty string in a case like ":LABEL"
                del pair[0]

            if len(pair) < 2:
                self.types[idx] = convert_schema_type(pair[0].casefold())
                self.skip_offsets[idx] = True
                if self.types[idx] not in (Type.ID, Type.START_ID, Type.END_ID, Type.IGNORE):
                    # Any other field should have 2 elements
                    raise SchemaError("Each property in the header should be a colon-separated pair")
            else:
                self.column_names[idx] = pair[0]
                self.types[idx] = convert_schema_type(pair[1].casefold())
                if self.types[idx] in (Type.START_ID, Type.END_ID, Type.IGNORE):
                    self.skip_offsets[idx] = True

        # The number of properties is equal to the number of non-skipped columns.
        self.prop_count = self.skip_offsets.count(False)
        self.packed_header = self.pack_header()
        self.binary_size += len(self.packed_header)

    # Convert a list of properties into a binary string
    def pack_props(self, line):
        props = []
        for idx, field in enumerate(line):
            if self.skip_offsets[idx]:
                continue
            if self.column_names[idx]:
                props.append(prop_to_binary(field, self.types[idx]))
        return b''.join(p for p in props)

    def to_binary(self):
        return self.packed_header + b''.join(self.binary_entities)
