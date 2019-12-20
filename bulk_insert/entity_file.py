import os
import io
import csv
import struct
import module_vars
from exceptions import CSVError
import schema


# Convert a single CSV property field into a binary stream.
# Supported property types are string, numeric, boolean, and NULL.
# type is either Type.DOUBLE, Type.BOOL or Type.STRING, and explicitly sets the value to this type if possible
def prop_to_binary(prop_val, type):
    # All format strings start with an unsigned char to represent our Type enum
    format_str = "=B"
    if prop_val is None:
        # An empty field indicates a NULL property
        return struct.pack(format_str, Type.NULL)

    # If field can be cast to a float, allow it
    if type == None or type == Type.DOUBLE:
        try:
            numeric_prop = float(prop_val)
            if not math.isnan(numeric_prop) and not math.isinf(numeric_prop): # Don't accept non-finite values.
                return struct.pack(format_str + "d", Type.DOUBLE, numeric_prop)
        except:
            pass

    if type == None or type == Type.BOOL:
        # If field is 'false' or 'true', it is a boolean
        if prop_val.lower() == 'false':
            return struct.pack(format_str + '?', Type.BOOL, False)
        elif prop_val.lower() == 'true':
            return struct.pack(format_str + '?', Type.BOOL, True)

    if type == None or type == Type.STRING:
        # If we've reached this point, the property is a string
        encoded_str = str.encode(prop_val) # struct.pack requires bytes objects as arguments
        # Encoding len+1 adds a null terminator to the string
        format_str += "%ds" % (len(encoded_str) + 1)
        return struct.pack(format_str, schema.Type.STRING, encoded_str)

    # If it hasn't returned by this point, it is trying to set it to a type that it can't adopt
    raise Exception("unable to parse [" + prop_val + "] with type ["+repr(type)+"]")


# Superclass for label and relation CSV files
class EntityFile(object):
    def __init__(self, filename, separator):
        # The label or relation type string is the basename of the file
        self.entity_str = os.path.splitext(os.path.basename(filename))[0]
        # Input file handling
        self.infile = io.open(filename, 'rt')
        # Initialize CSV reader that ignores leading whitespace in each field
        # and does not modify input quote characters
        self.reader = csv.reader(self.infile, delimiter=separator, skipinitialspace=True, quoting=module_vars.QUOTING)

        self.prop_offset = 0 # Starting index of properties in row
        self.prop_count = 0 # Number of properties per entity

        self.packed_header = b''
        self.binary_entities = []
        self.binary_size = 0 # size of binary token
        self.count_entities() # number of entities/row in file.

    # Count number of rows in file.
    def count_entities(self):
        self.entities_count = 0
        self.entities_count = sum(1 for line in self.infile)
        # discard header row
        self.entities_count -= 1
        # seek back
        self.infile.seek(0)
        return self.entities_count

    # Simple input validations for each row of a CSV file
    def validate_row(self, expected_col_count, row):
        # Each row should have the same number of fields
        if len(row) != expected_col_count:
            raise CSVError("%s:%d Expected %d columns, encountered %d ('%s')"
                           % (self.infile.name, self.reader.line_num, expected_col_count, len(row), ','.join(row)))

    # If part of a CSV file was sent to Redis, delete the processed entities and update the binary size
    def reset_partial_binary(self):
        self.binary_entities = []
        self.binary_size = len(self.packed_header)

    # Convert property keys from a CSV file header into a binary string
    def pack_header(self, header):
        prop_count = len(header) - self.prop_offset
        # String format
        entity_bytes = self.entity_str.encode()
        fmt = "=%dsI" % (len(entity_bytes) + 1) # Unaligned native, entity name, count of properties
        args = [entity_bytes, prop_count]
        for p in header[self.prop_offset:]:
            prop = p.encode()
            fmt += "%ds" % (len(prop) + 1) # encode string with a null terminator
            args.append(prop)
        return struct.pack(fmt, *args)

    # Convert a list of properties into a binary string
    def pack_props(self, line):
        props = []
        for num, field in enumerate(line[self.prop_offset:]):
            field_type_idx = self.prop_offset+num
            try:
                module_vars.FIELD_TYPES[self.entity_str][field_type_idx]
            except:
                props.append(prop_to_binary(field, None))
            else:
                props.append(prop_to_binary(field, module_vars.FIELD_TYPES[self.entity_str][field_type_idx]))
        return b''.join(p for p in props)

    def to_binary(self):
        return self.packed_header + b''.join(self.binary_entities)
