package org.trafodion.jdbc.t4;
import java.sql.SQLException;
public class TrafT4Savepoint implements java.sql.Savepoint {
    String savepointName;
    public TrafT4Savepoint(String savepointName) {
        this.savepointName = savepointName;
    }
    public TrafT4Savepoint() {
        this.savepointName = "";
    }
    @Override
    public int getSavepointId() throws SQLException {
        //TODO we need to make sure the id generated in driver or return from server
        return 0;
    }
    @Override
    public String getSavepointName() throws SQLException {
        return savepointName;
    }
}
