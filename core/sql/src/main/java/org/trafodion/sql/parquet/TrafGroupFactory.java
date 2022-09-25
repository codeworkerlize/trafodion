package org.trafodion.sql.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.schema.MessageType;

public class TrafGroupFactory extends GroupFactory {

  private final MessageType schema;

  public TrafGroupFactory(MessageType schema) {
    this.schema = schema;
  }

  @Override
  public Group newGroup() {
    return new TrafGroup(schema);
  }

}
