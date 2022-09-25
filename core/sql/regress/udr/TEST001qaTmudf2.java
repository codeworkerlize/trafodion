import java.io.*;
import org.trafodion.sql.udr.*;

public class TEST001qaTmudf2 extends UDR
{
  public TEST001qaTmudf2() {}

  @Override
  public void describeParamsAndColumns(UDRInvocationInfo info)
    throws UDRException
  {
    // Return the same input table back using addPassThruColumns().
    // add all input table columns as output columns (t is the entire table)
    for (int t = 0; t < info.getNumTableInputs(); t++)
      info.addPassThruColumns(t, 0, -1);

  }

  @Override
  public void processData(UDRInvocationInfo info,
                          UDRPlanInfo plan)
    throws UDRException
  {
    while (getNextRow(info)) {
      // copy the same columns and return the row
      info.copyPassThruData();
      emitRow(info);
    }
  }
}

