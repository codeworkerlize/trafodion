#!/bin/bash
#
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2016 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

nohup python $WMS_INSTALL_DIR/bin/scripts/wms_manager.py </dev/null >$TRAF_LOG/wms-nohup.log 2>&1 &
