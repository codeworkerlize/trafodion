package org.trafodion.dcs.master.listener.nio;

import java.io.IOException;

public class NoAvailServerException extends IOException {

    public NoAvailServerException(String msg) {
        super(msg);
    }
}
