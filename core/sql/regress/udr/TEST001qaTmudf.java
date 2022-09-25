import java.io.*;
import org.trafodion.sql.udr.*;

public class TEST001qaTmudf extends UDR
{
  public TEST001qaTmudf() {}

  @Override
  public void describeParamsAndColumns(UDRInvocationInfo info)
    throws UDRException
  {
    for (int i = 0; i < info.par().getNumColumns(); i++) {
      info.out().addCharColumn("VAL"+i, 15, true);
    }
  }

  @Override
  public void processData(UDRInvocationInfo info,
                          UDRPlanInfo plan)
    throws UDRException
  {
    int numParms = info.par().getNumColumns();

    for (int i = 0; i < numParms; i++) {
      double value = info.par().getDouble(i);
      info.out().setString(i, "value: " + value);
    }

    emitRow(info);
  }
}
