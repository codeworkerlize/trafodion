package org.trafodion.sql.parquet;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.example.data.Group;

import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import org.trafodion.sql.TrafParquetFileReader;

import org.trafodion.sql.parquet.TrafRecordConverter;

//public class TrafReadSupport extends ReadSupport<Group> {
public class TrafReadSupport extends GroupReadSupport {

    private TrafParquetFileReader m_tpfr = null;

    public void setTPFR(TrafParquetFileReader p_tpfr) {
	m_tpfr = p_tpfr;
    }

    public TrafParquetFileReader getTPFR() {
	return m_tpfr;
    }

    @Override
	public ReadSupport.ReadContext init(
					    Configuration configuration, 
					    Map<String, String> keyValueMetaData,
					    MessageType fileSchema) 
    {
	String partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA);
	MessageType requestedProjection = getSchemaForRead(fileSchema, partialSchemaString);
	return new ReadContext(requestedProjection);
    }

    @Override
	public RecordMaterializer<Group> 
	prepareForRead(Configuration configuration,
		       Map<String, String> keyValueMetaData, 
		       MessageType fileSchema,
		       org.apache.parquet.hadoop.api.ReadSupport.ReadContext readContext) 
    {
	//      System.out.println("TRS - prepareForRead()");
	TrafRecordConverter lv_trc = new TrafRecordConverter(readContext.getRequestedSchema());
	lv_trc.setTPFR(m_tpfr);
	return lv_trc;
    }

}
