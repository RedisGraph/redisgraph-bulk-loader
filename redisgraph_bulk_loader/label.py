import re
import sys
import click
from config import Config
import query_buffer as QueryBuffer
from entity_file import Type, EntityFile
from exceptions import SchemaError


# Handler class for processing label csv files.
class Label(EntityFile):
    def __init__(self, infile, label_str):
        self.id_namespace = None
        super(Label, self).__init__(infile, label_str)
        #  self.post_process_header()

    def process_schemaless_header(self, header):
        # The first column is the ID.
        # If this starts with an underscore, it is not a property and should not be introduced to the graph.
        self.types[0] = Type.ID
        self.id = 0
        if header[0][0] == '_':
            self.skip_offsets[0] = True
        #  self.types[1:] = [Type.INFERRED] * self.column_count - 1

        for idx, field in enumerate(header):
            self.column_names[idx] = field

    def post_process_header(self, header):
        # Verify that exactly one field is labeled ID.
        if self.types.count(Type.ID) != 1:
            raise SchemaError("Node file '%s' should have exactly one ID column."
                              % (self.infile.name))
        self.id = self.types.index(Type.ID) # Track the offset containing the node ID.
        id_field = header[self.id]
        # If the ID field specifies an ID namespace in parentheses like "val:ID(NAMESPACE)", capture the namespace.
        match = re.search(r"\((\w+)\)", id_field)
        if match:
            self.id_namespace = match.group(1)

    def process_entities(self):
        entities_created = 0
        with click.progressbar(self.reader, length=self.entities_count, label=self.entity_str) as reader:
            for row in reader:
                self.validate_row(row)
                # Add identifier->ID pair to dictionary if we are building relations
                if QueryBuffer.nodes is not None:
                    id_field = row[self.id]
                    if self.id_namespace is not None:
                        id_field = self.id_namespace + '.' + str(id_field)

                    if id_field in QueryBuffer.nodes:
                        sys.stderr.write("Node identifier '%s' was used multiple times - second occurrence at %s:%d\n"
                                         % (row[self.id], self.infile.name, self.reader.line_num))
                        if Config.skip_invalid_nodes is False:
                            sys.exit(1)
                    QueryBuffer.nodes[id_field] = QueryBuffer.top_node_id
                    QueryBuffer.top_node_id += 1
                row_binary = self.pack_props(row)
                row_binary_len = len(row_binary)
                # If the addition of this entity will make the binary token grow too large,
                # send the buffer now.
                if self.binary_size + row_binary_len > Config.max_token_size:
                    QueryBuffer.labels.append(self.to_binary())
                    QueryBuffer.send_buffer()
                    self.reset_partial_binary()
                    # Push the label onto the query buffer again, as there are more entities to process.
                    QueryBuffer.labels.append(self.to_binary())

                QueryBuffer.node_count += 1
                entities_created += 1
                self.binary_size += row_binary_len
                self.binary_entities.append(row_binary)
            QueryBuffer.labels.append(self.to_binary())
        self.infile.close()
        print("%d nodes created with label '%s'" % (entities_created, self.entity_str))
