package org.apache.hadoop.hbase.client.transactional;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class SQLInterface implements AutoCloseable {
  private final Process sqlciProcess;
  private final BufferedWriter sqlciWriter;
  private final BufferedReader sqlciReader;

  static final Log LOG = LogFactory.getLog(SQLInterface.class);

  public static class ResultSet {
    private final String[] column_meta;
    private final ArrayList<ArrayList<String>> store;
    private static int SQLCI_COLUMN_SEP_WIDTH = 2;

    private ResultSet(final String[] cm, final int size) {
      column_meta = cm;
      store = new ArrayList<>(size);
    }

    public int getColumnNumber() {
      return column_meta.length;
    }

    public int getRowCount() {
      return store.size();
    }

    public String getData(final int r, final int c) {
      return store.get(r).get(c);
    }

    private void parseRawLine(final String rawLine) {
      LOG.trace(String.format("SQLInterface parse raw line: {len:%s}{%s}", rawLine.length(), rawLine));
      if (rawLine.length() == 0 || rawLine.startsWith("---") || rawLine.startsWith("==="))
        return;

      final ArrayList<String> row = new ArrayList<>(getColumnNumber());
      int column_beg = 0;

      for (int i = 0; i < column_meta.length - 1; ++i) {
        row.add(rawLine.substring(column_beg, column_beg + column_meta[i].length()).trim());
        column_beg += column_meta[i].length() + SQLCI_COLUMN_SEP_WIDTH;
      }
      row.add(rawLine.substring(column_beg).trim());
      store.add(row);
    }

    public static ResultSet parseRawOutput(final List<String> rawout) throws IOException {
      final Iterator<String> it = rawout.iterator();
      final String[] first3Lines = new String[3];

      int headLineNum = 0;
      do {
        if (it.hasNext()) {
          String line = it.next();
          //due to Mantis 18195, it is possible that replay of DDL will fail with error 4082
          if (line.trim().startsWith("*** WARNING") || line.trim().startsWith("*** ERROR[4082]") )
            continue;
          first3Lines[headLineNum++] = line;
        }
        else {
          if (headLineNum == 2 && first3Lines[1].trim().equals("--- 0 row(s) selected.")) {
            return null;
          }
          else {
            throw new IOException("execute sql error: raw output less 3 three line, not result set.");
          }
        }
      } while (headLineNum < 3);

      if (!first3Lines[0].trim().equals(">>")) {
        LOG.error("***ERROR sqlci first line not >>, is: " + first3Lines[0]);
        throw new IOException("parse raw data error, sqlci first line not >>, is" + first3Lines[0]);
      }

      if (first3Lines[2].length() == 0 || (first3Lines[2].charAt(0) != '-' && first3Lines[2].charAt(0) != '=')) {
        throw new IOException("execute sql error: line 2 not start with [-=], not result set.");
      }

      final String[] columns_meta =  first3Lines[2].split("  ");

      final ResultSet resultSet = new ResultSet(columns_meta, rawout.size());

      // header
      resultSet.parseRawLine(first3Lines[1]);
      while (it.hasNext()) {
        final String line = it.next();
        if (line.trim().startsWith("---"))
          break;
        resultSet.parseRawLine(line);
      }
      return resultSet;
    }
  }

  public SQLInterface() throws IOException {
    final ProcessBuilder builder = new ProcessBuilder("sqlci");
    builder.redirectErrorStream(true);
    sqlciProcess = builder.start();

    final OutputStream stdin = sqlciProcess.getOutputStream();
    final InputStream stdout = sqlciProcess.getInputStream();

    sqlciWriter = new BufferedWriter(new OutputStreamWriter(stdin));
    sqlciReader = new BufferedReader(new InputStreamReader(stdout));

    // setup connection
    sqlciWriter.write("set param ?sqlciready ready;show param;\n");
    sqlciWriter.flush();

    for (String line; (line = sqlciReader.readLine()) != null;) {
      LOG.trace(line);
      if (line.trim().endsWith("?sqlciready ready"))
        break;
    }
    LOG.info("sqlci interface ready.");
  }

  public ResultSet executeQueryAndFetchResultSet(final String sql) throws IOException {
    try {
      final List<String> rawout = executeQueryAndGetRawOutput(sql);
      return ResultSet.parseRawOutput(rawout);
    }
    catch (IOException e) {
      throw new IOException("execute sql error, sql: " + sql, e);
    }
  }

  public void executeQuery(final String sql) throws IOException {
    final List<String> rawout = executeQueryAndGetRawOutput(sql);

    for (final String line : rawout) {
      if (line.trim().startsWith("*** ERROR")) {
        for (final String errlog : rawout) {
          LOG.error(errlog);
        }
        throw new IOException(String.format("execute the sql: %s, get error: %s", sql, line));
      }
    }
  }

  public List<String> executeQueryAndGetRawOutput(final String sql) throws IOException {
    final List<String> ret = new LinkedList<>();
    sendSqlciQuery(sql + ";\nshow param;\n");
    for (String line; (line = sqlciReader.readLine()) != null;) {
      if (line.trim().endsWith("?sqlciready ready"))
        break;
      ret.add(line);
    }
    return ret;
  }

  @Override
  public void close () throws IOException, InterruptedException {
    sendSqlciQuery("exit;\n");
    sqlciProcess.waitFor();
  }

  private void sendSqlciQuery(final String query) throws IOException {
    sqlciWriter.write(query);
    sqlciWriter.flush();
  }

  private static void replaySequence() throws IOException {
    try (final SQLInterface sqlci = new SQLInterface()) {
      sqlci.executeQuery("set parserflags 131072");
      SQLInterface.ResultSet res = sqlci.executeQueryAndFetchResultSet("select * from \"_XDC_MD_\".XDC_SEQ");

      if (res != null) {
        for (int r = 1; r < res.getRowCount(); ++r) {
          String alter_query = String.format("ALTER SEQUENCE %s.%s.%s SET NEXTVAL %s",
                                             res.getData(r, 0), res.getData(r, 1),
                                             res.getData(r, 2), res.getData(r, 3));
          sqlci.executeQuery(alter_query);
        }
      }
    }
    catch (Exception e) {
      LOG.error("SQLInterface replay sequence error.", e);
      throw new IOException("SQLInterface replay sequence error.", e);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length > 0) {
      if (args[0].equals("replaySequence")) {
        replaySequence();
      }
      else {
        System.err.println("unsupported command: " + args[0]);
      }
    }
    else { // for debug
      try (final SQLInterface sqlci = new SQLInterface();
           Scanner scan = new Scanner(System.in)) {

        System.out.print("SQL> ");
        while (scan.hasNext()) {
          final String inputLine = scan.nextLine();
          if (inputLine.startsWith("exit"))
            break;

          List<String> rawout = sqlci.executeQueryAndGetRawOutput(inputLine);
          for (String line : rawout) {
            System.out.println(line);
          }

          System.out.print("SQL> ");
        }
      }
    }
  }
}
