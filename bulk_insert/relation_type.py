import struct
import click
from entity_file import EntityFile
from exceptions import CSVError, SchemaError
from configs import Configs
from schema import Type


# Handler class for processing relation csv files.
class RelationType(EntityFile):
    def __init__(self, query_buf, infile):
        super(RelationType, self).__init__(infile, query_buf)
        if self.column_count < 2:
            raise CSVError("Relation file '%s' should have at least 2 elements in header line."
                           % (infile.name))

        self.start_id = -1
        self.end_id = -1
        self.post_process_header()

    def post_process_header(self):
        # Can interleave these tasks if preferred.
        if self.types.count(Type.START_ID) != 1:
            raise SchemaError("Relation file '%s' should have exactly one START_ID column."
                              % (self.infile.name))
        if self.types.count(Type.END_ID) != 1:
            raise SchemaError("Relation file '%s' should have exactly one END_ID column."
                              % (self.infile.name))

        self.start_id = self.types.index(Type.START_ID)
        self.end_id = self.types.index(Type.END_ID)

    def process_entities(self):
        entities_created = 0
        with click.progressbar(self.reader, length=self.entities_count, label=self.entity_str) as reader:
            for row in reader:
                self.validate_row(row)
                try:
                    src = self.query_buf.nodes[row[self.start_id]]
                    dest = self.query_buf.nodes[row[self.end_id]]
                except KeyError as e:
                    print("Relationship specified a non-existent identifier. src: %s; dest: %s" % (row[self.start_id], row[self.end_id]))
                    if Configs.skip_invalid_edges is False:
                        raise e
                    continue
                fmt = "=QQ" # 8-byte unsigned ints for src and dest
                row_binary = struct.pack(fmt, src, dest) + self.pack_props(row)
                row_binary_len = len(row_binary)
                # If the addition of this entity will make the binary token grow too large,
                # send the buffer now.
                if self.binary_size + row_binary_len > Configs.max_token_size:
                    self.query_buf.QUERY_BUF.reltypes.append(self.to_binary())
                    self.query_buf.QUERY_BUF.send_buffer()
                    self.reset_partial_binary()
                    # Push the reltype onto the query buffer again, as there are more entities to process.
                    self.query_buf.QUERY_BUF.reltypes.append(self.to_binary())

                self.query_buf.QUERY_BUF.relation_count += 1
                entities_created += 1
                self.binary_size += row_binary_len
                self.binary_entities.append(row_binary)
            self.query_buf.QUERY_BUF.reltypes.append(self.to_binary())
        self.infile.close()
        print("%d relations created for type '%s'" % (entities_created, self.entity_str))
