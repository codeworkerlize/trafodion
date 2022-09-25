// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.common;

// info on oversubscription
// -------------------------------------------
// NOTE: Keep this in sync with C++ class
//       OversubscriptionInfo in file
//       core/sql/executor/TenantHelper_JNI.h
// -------------------------------------------
public class OversubscriptionInfo {

    private int[] measures_;
    // indexes into the measures array

    // number of oversubscribed nodes (this is the
    // main indicator to check, if it is greater zero
    // that indicates that we are oversubscribed)
    public static int NUM_OVERSUBSCRIBED_NODES_IX    = 0;
    // number of nodes in the cluster
    public static int NUM_AVAILABLE_NODES_IX         = 1;
    // number of units/cubes/slices in the cluster,
    // max number that can be used without oversubscription
    public static int NUM_AVAILABLE_UNITS_IX         = 2;
    // number of units/cubes/slices actually allocated
    public static int NUM_ALLOCATED_UNITS_IX         = 3;
    // node id of one node that is oversubscribed
    // (or -1 if no node is oversubscribed)
    public static int MOST_OVERSUBSCRIBED_NODE_ID_IX = 4;
    // number of units available on that node, if
    // applicable
    public static int MOST_OVERSUBSCRIBED_AVAIL_IX   = 5;
    // number of actual units assigned to that node,
    // if applicable (will be greater than the previous
    // counter if such a node exists)
    public static int MOST_OVERSUBSCRIBED_ALLOC_IX   = 6;
    // update this total # of measures  when adding more
    private static int NUM_MEASURES                  = 7;

    public OversubscriptionInfo() {
        init();
    }

    public void init() {
        if (measures_ == null)
            measures_ = new int[NUM_MEASURES];
        for (int ix=0; ix<measures_.length; ix++)
            measures_[ix] = 0;
        measures_[MOST_OVERSUBSCRIBED_NODE_ID_IX] = -1;
    }

    public boolean isOversubscribed() {
        return (measures_[NUM_OVERSUBSCRIBED_NODES_IX] > 0);
    }

    public int getNumMeasures() {
        return NUM_MEASURES;
    }

    public void copyMeasures(int[] targetArray) {
        for (int ix2=0; ix2<targetArray.length; ix2++) {
            if (ix2 < measures_.length)
                targetArray[ix2] = measures_[ix2];
            else
                targetArray[ix2] = -1;
        }
    }

    public int getMeasure(int ix) {
        if (ix >= 0 && ix < NUM_MEASURES)
            return measures_[ix];
        else
            return -1;
    }

    public void setMeasure(int ix, int v) {
        if (ix >= 0 && ix < NUM_MEASURES)
            measures_[ix] = v;
        else
            throw new IllegalArgumentException(
                "Index of oversubscription counter is out of range");
    }

    public void addToMeasure(int ix, int v) {
        if (ix >= 0 && ix < NUM_MEASURES)
            measures_[ix] += v;
        else
            throw new IllegalArgumentException(
                "Index of oversubscription counter is out of range");
    }
}
