package org.trafodion.sql.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

class TrafGroupConverter extends GroupConverter {
  private final TrafGroupConverter parent;
  private final int index;
  protected TrafGroup current = null;
  private Converter[] converters;

  TrafGroupConverter(TrafGroupConverter parent, int index, GroupType schema) {
    this.parent = parent;
    this.index = index;
    this.current = new TrafGroup(schema);

    converters = new Converter[schema.getFieldCount()];

    for (int i = 0; i < converters.length; i++) {
      final Type type = schema.getType(i);
      if (type.isPrimitive()) {
        converters[i] = new TrafPrimitiveConverter(this, i);
      } else {
        converters[i] = new TrafGroupConverter(this, i, type.asGroupType());
      }

    }
  }

  @Override
  public void start() {
      current = parent.getCurrentRecord();
      current.addGroup(index);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void end() {
  }

  public TrafGroup getCurrentRecord() {
    return current;
  }
}
