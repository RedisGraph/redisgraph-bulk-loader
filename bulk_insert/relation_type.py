import struct
import click
from entity_file import EntityFile
import module_vars

# Handler class for processing relation csv files.
class RelationType(EntityFile):
    def __init__(self, infile, separator):
        super(RelationType, self).__init__(infile, separator)
        expected_col_count = self.process_header()
        self.process_entities(expected_col_count)
        self.infile.close()

    def process_header(self):
        # Header format:
        # source identifier, dest identifier, properties[0..n]
        header = next(self.reader)
        # Assume rectangular CSVs
        expected_col_count = len(header)
        self.prop_count = expected_col_count - 2
        if self.prop_count < 0:
            raise CSVError("Relation file '%s' should have at least 2 elements in header line."
                           % (self.infile.name))

        self.prop_offset = 2
        self.packed_header = self.pack_header(header) # skip src and dest identifiers
        self.binary_size += len(self.packed_header)
        return expected_col_count

    def process_entities(self, expected_col_count):
        entities_created = 0
        with click.progressbar(self.reader, length=self.entities_count, label=self.entity_str) as reader:
            for row in reader:
                self.validate_row(expected_col_count, row)
                try:
                    src = module_vars.NODE_DICT[row[0]]
                    dest = module_vars.NODE_DICT[row[1]]
                except KeyError as e:
                    print("Relationship specified a non-existent identifier. src: %s; dest: %s" % (row[0], row[1]))
                    if module_vars.CONFIGS.skip_invalid_edges is False:
                        raise e
                    continue
                fmt = "=QQ" # 8-byte unsigned ints for src and dest
                row_binary = struct.pack(fmt, src, dest) + self.pack_props(row)
                row_binary_len = len(row_binary)
                # If the addition of this entity will make the binary token grow too large,
                # send the buffer now.
                if self.binary_size + row_binary_len > module_vars.CONFIGS.max_token_size:
                    module_vars.QUERY_BUF.reltypes.append(self.to_binary())
                    module_vars.QUERY_BUF.send_buffer()
                    self.reset_partial_binary()
                    # Push the reltype onto the query buffer again, as there are more entities to process.
                    module_vars.QUERY_BUF.reltypes.append(self.to_binary())

                module_vars.QUERY_BUF.relation_count += 1
                entities_created += 1
                self.binary_size += row_binary_len
                self.binary_entities.append(row_binary)
            module_vars.QUERY_BUF.reltypes.append(self.to_binary())
        print("%d relations created for type '%s'" % (entities_created, self.entity_str))
