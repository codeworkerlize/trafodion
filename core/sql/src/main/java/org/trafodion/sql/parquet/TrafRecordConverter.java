package org.trafodion.sql.parquet;

import org.apache.parquet.example.data.Group;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import org.trafodion.sql.TrafParquetFileReader;

import org.trafodion.sql.parquet.TrafGroupConverter;
import org.trafodion.sql.parquet.TrafGroupFactory;
import org.trafodion.sql.parquet.TrafRecordConverter;

public class TrafRecordConverter extends RecordMaterializer<Group> {

  private TrafGroupConverter root;

  private TrafParquetFileReader m_tpfr = null;
  
  public TrafRecordConverter(MessageType schema) {
      //System.out.println("TRC.TRC");
      final MessageType lv_schema = schema;
      final TrafRecordConverter lv_trc_this = this;
      this.root = new TrafGroupConverter(null, 0, schema) {
      @Override
      public void start() {
	  /*
          System.out.println("TRC.TGC.start"
			     + ", trc_this: " + lv_trc_this
			     + ", tpfr: " + m_tpfr
			     );
	  */
	  //assuming that this.current is constructed in TrafGroupConverter
	  this.current.init(lv_trc_this);
      }

      @Override
      public void end() {
          //System.out.println("TRC.TGC.end");
	  this.current.end();
      }
    };
  }

  @Override
  public Group getCurrentRecord() {
      //      System.out.println("TRC.getCurrentRecord()");
    return root.getCurrentRecord();
  }

  @Override
  public GroupConverter getRootConverter() {
      //System.out.println("TRC.getRootConverter()");
    return root;
  }

  public void setTPFR(TrafParquetFileReader p_tpfr) {
      m_tpfr = p_tpfr;
  }
  
  public TrafParquetFileReader getTPFR() {
      return m_tpfr;
  }

}
