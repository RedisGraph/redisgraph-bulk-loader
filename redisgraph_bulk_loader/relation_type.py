import re
import struct
import click
from entity_file import Type, EntityFile
from exceptions import CSVError, SchemaError


# Handler class for processing relation csv files.
class RelationType(EntityFile):
    def __init__(self, query_buffer, infile, type_str, config):
        super(RelationType, self).__init__(infile, type_str, config)
        self.query_buffer = query_buffer

    def process_schemaless_header(self, header):
        if self.column_count < 2:
            raise CSVError("Relation file '%s' should have at least 2 elements in header line."
                           % (self.infile.name))
        # The first column is the source ID and the second is the destination ID.
        self.start_id = 0
        self.end_id = 1
        self.start_namespace = None
        self.end_namespace = None

        for idx, field in enumerate(header[2:]):
            self.column_names[idx+2] = field.strip()

    def post_process_header_with_schema(self, header):
        # Can interleave these tasks if preferred.
        if self.types.count(Type.START_ID) != 1:
            raise SchemaError("Relation file '%s' should have exactly one START_ID column."
                              % (self.infile.name))
        if self.types.count(Type.END_ID) != 1:
            raise SchemaError("Relation file '%s' should have exactly one END_ID column."
                              % (self.infile.name))

        self.start_id = self.types.index(Type.START_ID)
        self.end_id = self.types.index(Type.END_ID)
        self.start_namespace = None
        self.end_namespace = None
        # Capture namespaces of start and end IDs if provided
        start_match = re.search(r"\((\w+)\)", header[self.start_id])
        if start_match:
            self.start_namespace = start_match.group(1)
        end_match = re.search(r"\((\w+)\)", header[self.end_id])
        if end_match:
            self.end_namespace = end_match.group(1)

    def process_entities(self):
        entities_created = 0
        with click.progressbar(self.reader, length=self.entities_count, label=self.entity_str) as reader:
            for row in reader:
                self.validate_row(row)
                try:
                    start_id = row[self.start_id]
                    if self.start_namespace:
                        start_id = self.start_namespace + '.' + str(start_id)
                    end_id = row[self.end_id]
                    if self.end_namespace:
                        end_id = self.end_namespace + '.' + str(end_id)

                    src = self.query_buffer.nodes[start_id]
                    dest = self.query_buffer.nodes[end_id]
                except KeyError as e:
                    print("%s:%d Relationship specified a non-existent identifier. src: %s; dest: %s" %
                          (self.infile.name, self.reader.line_num - 1, row[self.start_id], row[self.end_id]))
                    if self.config.skip_invalid_edges is False:
                        raise e
                    continue
                fmt = "=QQ" # 8-byte unsigned ints for src and dest
                try:
                    row_binary = struct.pack(fmt, src, dest) + self.pack_props(row)
                except SchemaError as e:
                    raise SchemaError("%s:%d %s" % (self.infile.name, self.reader.line_num, str(e)))
                row_binary_len = len(row_binary)
                # If the addition of this entity will make the binary token grow too large,
                # send the buffer now.
                added_size = self.binary_size + row_binary_len
                if added_size >= self.config.max_token_size or self.query_buffer.buffer_size + added_size >= self.config.max_buffer_size:
                    self.query_buffer.reltypes.append(self.to_binary())
                    self.query_buffer.send_buffer()
                    self.reset_partial_binary()
                    # Push the reltype onto the query buffer again, as there are more entities to process.
                    self.query_buffer.reltypes.append(self.to_binary())

                self.query_buffer.relation_count += 1
                entities_created += 1
                self.binary_size += row_binary_len
                self.binary_entities.append(row_binary)
            self.query_buffer.reltypes.append(self.to_binary())
        self.infile.close()
        print("%d relations created for type '%s'" % (entities_created, self.entity_str))
