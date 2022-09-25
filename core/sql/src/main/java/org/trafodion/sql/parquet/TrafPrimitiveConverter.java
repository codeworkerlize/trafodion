package org.trafodion.sql.parquet;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

class TrafPrimitiveConverter extends PrimitiveConverter {

  private final TrafGroupConverter parent;
  private final int index;

  TrafPrimitiveConverter(TrafGroupConverter parent, int index) {
    this.parent = parent;
    this.index = index;
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addBinary(Binary)
   */
  @Override
  public void addBinary(Binary value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addBoolean(boolean)
   */
  @Override
  public void addBoolean(boolean value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addDouble(double)
   */
  @Override
  public void addDouble(double value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addFloat(float)
   */
  @Override
  public void addFloat(float value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addInt(int)
   */
  @Override
  public void addInt(int value) {
      //System.out.println("TPC.addInt()");
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addLong(long)
   */
  @Override
  public void addLong(long value) {
    parent.getCurrentRecord().add(index, value);
  }

}
