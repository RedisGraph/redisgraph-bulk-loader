import ast
import csv
import io
import math
import os
import struct
import sys
from enum import Enum

try:
    from .exceptions import CSVError, SchemaError
except:
    from exceptions import CSVError, SchemaError

csv.field_size_limit(sys.maxsize)  # Don't limit the size of user input fields.


class Type(Enum):
    UNKNOWN = 0
    BOOL = 1
    BOOLEAN = 1  # alias to BOOL
    DOUBLE = 2
    FLOAT = 2  # alias to DOUBLE
    STRING = 3
    LONG = 4
    INT = 4  # alias to LONG
    INTEGER = 4  # alias to LONG
    ARRAY = 5
    ID_STRING = 6
    ID_INTEGER = 7
    START_ID = 8
    END_ID = 9
    IGNORE = 10


def convert_schema_type(in_type):
    try:
        return Type[in_type]
    except KeyError:
        # Handling for ID namespaces
        # TODO think of better alternatives
        if in_type.startswith("ID"):
            return Type.ID_STRING
        elif in_type.startswith("START_ID("):
            return Type.START_ID
        elif in_type.startswith("END_ID("):
            return Type.END_ID
        else:
            raise SchemaError(f"Encountered invalid field type '{in_type}'")


def array_prop_to_binary(format_str, prop_val):
    # Evaluate the array to convert its elements.
    # (This allows us to handle nested arrays.)
    array_val = ast.literal_eval(prop_val)
    # Send array length as a long.
    array_to_send = struct.pack(format_str + "q", Type.ARRAY.value, len(array_val))
    # Recursively send each array element as a string.
    for elem in array_val:
        array_to_send += inferred_prop_to_binary(str(elem))
        # Return the full array struct.
    return array_to_send


# Convert a property field with an enforced type into a binary stream.
# Supported property types are string, integer, float, and boolean.
def typed_prop_to_binary(prop_val, prop_type):
    # All format strings start with an unsigned char to represent our prop_type enum
    format_str = "=B"

    # Remove leading and trailing whitespace
    prop_val = prop_val.strip()

    if prop_val == "":
        # An empty string indicates a NULL property.
        # TODO This is not allowed in Cypher, consider how to handle it here rather than in-module.
        return struct.pack(format_str, 0)

    if prop_type == Type.ID_INTEGER or prop_type == Type.LONG:
        try:
            numeric_prop = int(prop_val)
            return struct.pack(format_str + "q", Type.LONG.value, numeric_prop)
        except (ValueError, struct.error):
            # TODO ugly, rethink
            if prop_type == Type.LONG:
                raise SchemaError(f"Could not parse '{prop_val}' as a long")

    elif prop_type == Type.DOUBLE:
        try:
            numeric_prop = float(prop_val)
            if not math.isnan(numeric_prop) and not math.isinf(
                numeric_prop
            ):  # Don't accept non-finite values.
                return struct.pack(format_str + "d", Type.DOUBLE.value, numeric_prop)
        except (ValueError, struct.error):
            # TODO ugly, rethink
            if prop_type == Type.DOUBLE:
                raise SchemaError(f"Could not parse '{prop_val}' as a double")

    elif prop_type == Type.BOOL:
        # If field is 'false' or 'true', it is a boolean
        if prop_val.lower() == "false":
            return struct.pack(format_str + "?", Type.BOOL.value, False)
        elif prop_val.lower() == "true":
            return struct.pack(format_str + "?", Type.BOOL.value, True)
        else:
            raise SchemaError(f"Could not parse '{prop_val}' as a boolean")

    elif prop_type == Type.ID_STRING or prop_type == Type.STRING:
        # If we've reached this point, the property is a string
        encoded_str = str.encode(
            prop_val
        )  # struct.pack requires bytes objects as arguments
        # Encoding len+1 adds a null terminator to the string
        format_str += "%ds" % (len(encoded_str) + 1)
        return struct.pack(format_str, Type.STRING.value, encoded_str)

    elif prop_type == Type.ARRAY:
        if prop_val[0] != "[" or prop_val[-1] != "]":
            raise SchemaError(f"Could not parse '{prop_val}' as an array")
        return array_prop_to_binary(format_str, prop_val)

    # If it hasn't returned by this point, it is trying to set it to a type that it can't adopt
    raise SchemaError(
        "unable to parse [" + prop_val + "] with type [" + repr(prop_type) + "]"
    )


# Convert a single CSV property field with an inferred type into a binary stream.
# Supported property types are string, integer, float, boolean, and (erroneously) null.
def inferred_prop_to_binary(prop_val):
    # All format strings start with an unsigned char to represent our prop_type enum
    format_str = "=B"

    # Remove leading and trailing whitespace
    prop_val = prop_val.strip()

    if prop_val == "":
        # An empty string indicates a NULL property.
        # TODO This is not allowed in Cypher, consider how to handle it here rather than in-module.
        return struct.pack(format_str, 0)

    # Try to parse value as an integer.
    try:
        numeric_prop = int(prop_val)
        return struct.pack(format_str + "q", Type.LONG.value, numeric_prop)
    except (ValueError, struct.error):
        pass

    # Try to parse value as a float.
    try:
        numeric_prop = float(prop_val)
        if not math.isnan(numeric_prop) and not math.isinf(
            numeric_prop
        ):  # Don't accept non-finite values.
            return struct.pack(format_str + "d", Type.DOUBLE.value, numeric_prop)
    except (ValueError, struct.error):
        pass

    # If field is 'false' or 'true', it is a boolean.
    if prop_val.lower() == "false":
        return struct.pack(format_str + "?", Type.BOOL.value, False)
    elif prop_val.lower() == "true":
        return struct.pack(format_str + "?", Type.BOOL.value, True)

    # If the property string is bracket-interpolated, it is an array.
    if prop_val[0] == "[" and prop_val[-1] == "]":
        try:
            return array_prop_to_binary(format_str, prop_val)
        except Exception:
            pass

    # If we've reached this point, the property is a string.
    encoded_str = str.encode(
        prop_val
    )  # struct.pack requires bytes objects as arguments
    # Encoding len+1 adds a null terminator to the string
    format_str += "%ds" % (len(encoded_str) + 1)
    return struct.pack(format_str, Type.STRING.value, encoded_str)


class EntityFile(object):
    """Superclass for Label and RelationType classes"""

    def __init__(self, filename, label, config, filter_column=None):
        # The configurations for this run.
        self.config = config

        # The label or relation type string is the basename of the file
        if label:
            self.entity_str = label
        else:
            self.entity_str = os.path.splitext(os.path.basename(filename))[0]
        # Input file handling
        self.infile = io.open(filename, "rt")

        # Initialize CSV reader that ignores leading whitespace in each field
        # and does not modify input quote characters
        self.reader = csv.reader(
            self.infile,
            delimiter=config.separator,
            skipinitialspace=True,
            quoting=config.quoting,
            escapechar=config.escapechar,
        )

        self.packed_header = b""
        self.binary_entities = []
        self.binary_size = 0  # size of binary token
        
        self.convert_header()  # Extract data from header row.
        self.count_entities()  # Count number of entities/row in file.
        
        if filter_column is None:
            self.__FILTER_ID__ =  -1
            self.__FILTER_VALUE__ = None
        else:
            try:
                self.__FILTER_ID__ = self.column_names.index(filter_column[0])
                self.__FILTER_VALUE__ = filter_column[1]
            except ValueError:  # it doesn't have to apply in the multiple file case
                self.__FILTER_ID__ =  -1
                self.__FILTER_VALUE__ = None
 
        next(self.reader)  # Skip the header row.
        
    @property
    def filter_value(self):
        return self.__FILTER_VALUE__
    
    @property
    def filter_column_id(self):
        return self.__FILTER_ID__

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
            raise CSVError(
                "%s:%d Expected %d columns, encountered %d ('%s')"
                % (
                    self.infile.name,
                    self.reader.line_num,
                    self.column_count,
                    len(row),
                    self.config.separator.join(row),
                )
            )

    # If part of a CSV file was sent to Redis, delete the processed entities and update the binary size
    def reset_partial_binary(self):
        self.binary_entities = []
        self.binary_size = len(self.packed_header)

    # Convert property keys from a CSV file header into a binary string
    def pack_header(self):
        # String format
        entity_bytes = self.entity_str.encode()
        fmt = "=%dsI" % (
            len(entity_bytes) + 1
        )  # Unaligned native, entity name, count of properties
        args = [entity_bytes, self.prop_count]
        for idx in range(self.column_count):
            if not self.column_names[idx]:
                continue
            prop = self.column_names[idx].encode()
            fmt += "%ds" % (len(prop) + 1)  # encode string with a null terminator
            args.append(prop)
        return struct.pack(fmt, *args)

    def convert_header_with_schema(self, header):
        self.types = [None] * self.column_count  # Value type of every column.
        for idx, field in enumerate(header):
            pair = field.split(":")

            # Multiple colons found in column name, emit error.
            # TODO might need to check for backtick escapes
            if len(pair) > 2:
                raise CSVError(
                    f"{self.infile.name}: Field '{field}' had {len(field)} colons"
                )

            # Convert the column type.
            col_type = convert_schema_type(pair[1].upper().strip())

            # If the column did not have a name but the type requires one, emit an error.
            if len(pair[0]) == 0 and col_type not in (
                Type.ID_STRING,
                Type.ID_INTEGER,
                Type.START_ID,
                Type.END_ID,
                Type.IGNORE,
            ):
                raise SchemaError(
                    f"{self.infile.name}: Each property in the header should be a colon-separated pair"
                )
            else:
                # We have a column name and a type.
                # Only store the name if the column's values should be added as properties.
                if len(pair[0]) > 0 and col_type not in (
                    Type.START_ID,
                    Type.END_ID,
                    Type.IGNORE,
                ):
                    column_name = pair[0].strip()
                    self.column_names[idx] = column_name

            # ID types may be parsed as strings or integers depending on user specification.
            if col_type == Type.ID_STRING and self.config.id_type == "INTEGER":
                col_type = Type.ID_INTEGER

            # Store the column type.
            self.types[idx] = col_type

    def convert_header(self):
        header = next(self.reader)
        self.column_count = len(header)
        self.column_names = [
            None
        ] * self.column_count  # Property names of every column; None if column does not update graph.

        if self.config.enforce_schema:
            # Use generic logic to convert the header with schema.
            self.convert_header_with_schema(header)
            # The subclass will perform post-processing.
            self.post_process_header_with_schema(header)
        else:
            # The subclass will process the header itself
            self.process_schemaless_header(header)

        # The number of properties is equal to the number of non-skipped columns.
        self.prop_count = self.column_count - self.column_names.count(None)
        self.packed_header = self.pack_header()
        self.binary_size += len(self.packed_header)

    # Convert a list of properties into a binary string
    def pack_props(self, line):
        props = []
        for idx, field in enumerate(line):
            if not self.column_names[idx]:
                continue
            if self.config.enforce_schema:
                props.append(typed_prop_to_binary(field, self.types[idx]))
            else:
                props.append(inferred_prop_to_binary(field))
        return b"".join(p for p in props)

    def to_binary(self):
        return self.packed_header + b"".join(self.binary_entities)
