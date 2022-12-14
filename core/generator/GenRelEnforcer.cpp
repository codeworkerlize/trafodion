
/* -*-C++-*-
******************************************************************************
*
* File:         GenRelEnforcer.C
* Description:  Generating executor object for enforcer nodes
*
*
* Created:      5/17/94
* Language:     C++
*
*
*
*
******************************************************************************
*/

// -----------------------------------------------------------------------

#include "generator/GenExpGenerator.h"
#include "optimizer/RelEnforcer.h"
#include "common/ComOptIncludes.h"
#include "generator/Generator.h"
#include "optimizer/GroupAttr.h"
#include "optimizer/RelMisc.h"
#include "optimizer/RelRoutine.h"
#include "optimizer/RelUpdate.h"
//#include "executor/ex_stdh.h"
#include "comexe/ComTdb.h"
#include "exp/ExpCriDesc.h"
//#include "executor/ex_tcb.h"
#include "common/ComCextdecs.h"

#include "comexe/ComTdbSendBottom.h"
#include "comexe/ComTdbSendTop.h"
#include "comexe/ComTdbSplitBottom.h"
#include "comexe/ComTdbSplitTop.h"
#include "sqlcomp/DefaultConstants.h"

/////////////////////////////////////////////////////////////////////
//
// Contents:
//
//   Exchange::codeGen()
//   Exchange::codeGenForSplitTop()
//   Exchange::codeGenForESP()
//
//////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////
//
// Exchange::codeGen()
//
//////////////////////////////////////////////////////////////////////
short Exchange::codeGen(Generator *generator) {
  ////////////////////////////////////////////////////////////////////////
  // In the case where there is no ESP involved, when we simply want to
  // parallelize the data stream that comes back from DP2 without having
  // to repartition, then produce a split top node only. In all other
  // cases, produce four nodes: split top, send top, send bottom, and
  // split bottom. If we create a split top node by itself, then the split
  // top node is responsible for producing partition input values,
  // otherwise the split bottom node expects to receive the partition
  // input values in a message.
  ////////////////////////////////////////////////////////////////////////
  if (isDP2Exchange()) {
    GenAssert(isAPAPA(), "PA should have eliminated its exchange");
    return codeGenForSplitTop(generator);
  } else {
    return codeGenForESP(generator);
  }
}

short Exchange::codeGenForSplitTop(Generator *generator) {
  GenAssert(FALSE, "Split top PA node not supported");
  return 0;
}

short Exchange::codeGenForESP(Generator *generator) {
  ////////////////////////////////////////////////////////////////////////////
  //
  // Case of generating a fragment to be downloaded into an ESP:
  //
  //         split top     |
  //             |         |- top ESP(s) or master executor (current fragment)
  //         send top      |
  //
  //
  //        send bottom    |
  //             |         |
  //       split bottom    |- bottom ESP(s) (new fragment)
  //             |         |
  //        child tree     |
  //
  ////////////////////////////////////////////////////////////////////////////
  ExpGenerator *expGen = generator->getExpGenerator();
  MapTable *mapTable = generator->getMapTable();
  Space *space = generator->getSpace();
  FragmentDir *fragmentDir = generator->getFragmentDir();
  MapTable *parentsSavedMapTable;

  ////////////////////////////////////////////////////////////////////////////
  //
  // Returned atp layout (to parent of split top node):
  //
  // |-------------|   |------------------------------|
  // | input data  |   | input data  |   output row   |
  // | ( I tupps ) |   | ( I tupps ) |   ( 1 tupp )   |
  // |-------------|   |------------------------------|
  // <- input ATP ->   <-- returned ATP to parent ---->
  //
  // input data:        the atp input to this node by its parent.
  // output row:        tupp where the row read from ESP is moved.
  //
  // The ATP layout between split top and send top nodes is the same as
  // the format between the split top node and its parent (the split top
  // node does not change the ATP format).
  //
  // ATP layout between send bottom and split bottom node:
  //
  // |---------------------|   |-----------------------------------|
  // |     input data      |   |   input data    |   output row    |
  // |     ( 1 tupp )      |   |   ( 2 tupps )   |   ( J tupps )   |
  // |---------------------|   |-----------------------------------|
  // <-- ATP sent down
  //     to split bottom -->   <-- returned ATP to send bottom ---->
  //
  // In other words, the split bottom node adds the partition input tuple
  // to the requests that it gets from the send bottom nodes. Note that
  // it does not strip the returned partition input values away before
  // passing the output rows back to the send bottom node.
  //
  // ATP layout of queues between split bottom node and its child
  // (not including the mandatory consts and temps entries)
  //
  // |---------------------------------|   |------------------------------|
  // | input data  |  part. input data |   | input data  |  output row    |
  // | ( 1 tupp )  |    ( 1 tupp )     |   | ( 2 tupps ) |  ( J tupps )   |
  // |---------------------------------|   |------------------------------|
  // <----- ATP sent down to child ---->   <-- returned ATP to parent ---->
  //
  // 2 input tupps (one is received from the parent, another one is received
  //                from the ESP manager when a partition got assigned to
  //                this ESP)
  // J output tupps (whatever the split bottom's child created)
  //
  // The messages that are exchanged between split top and split bottom
  // nodes contain 1 input tupp and 1 output tupp. The input contains
  // the characteristic inputs of the exchange node, the output contains
  // the characteristic outputs of the child of the exchange node.
  //
  // The split top node uses a work atp to encode a merge key from a child
  // row. It returns child rows in binary ascending order of the encoded keys.
  // Another tupp in its work atp is used to hold the result of the input
  // partitioning function:
  //
  // |---------------------------------------------|
  // | consts | temps  | merge key | input part no |
  // | 1 tupp | 1 tupp |  1 tupp   |    1 tupp     |
  // |---------------------------------------------|
  // <------------- split top work ATP ------------>
  //
  // The send top node uses a work atp to store the one input tupp that
  // goes down to the child (note that there is no down queue in the send
  // top node).
  //
  // |-----------------------------|
  // | consts | temps  | down tupp |
  // | 1 tupp | 1 tupp |   1 tupp  |
  // |-----------------------------|
  // <----- send top work ATP ----->
  //
  // The send bottom node uses a work ATP, too. Its work ATP contains
  // 1 tupp, describing the information sent up in the message (sql table row)
  // in addition to the mandatory consts and temps entries.
  //
  // |---------------------------------|
  // | consts | temps  |  return tupp  |
  // | 1 tupp | 1 tupp |     1 tupp    |
  // |---------------------------------|
  // <------ send bottom work ATP ----->
  //
  // Finally, the split bottom node also has a work ATP. It holds the
  // partition input data and a tupp that is used to calculate the output
  // partition number of rows that come up from the children:
  //
  // |-------------------------------------------------------------------|
  // | consts | temps  | part. input data | output part no | conv error  |
  // | 1 tupp | 1 tupp |      1 tupp      |     1 tupp     |   1 tupp    |
  // |-------------------------------------------------------------------|
  // <---------------------- split bottom work ATP ---------------------->
  //
  // Both input and output partition number are signed 32 bit quantities
  // (C++ "long" datatype) that cannot be NULL.
  //
  ////////////////////////////////////////////////////////////////////////////

  // work atp of split top node
  const int mergeTuppAtp = 1;       // work atp
  const int mergeTuppAtpIndex = 2;  // after consts and temps
  const int inputPartNoAtp = 1;     // work ATP
  const int inputPartNoAtpIndex = mergeTuppAtpIndex + 1;
  const int splitTopWorkTupps = inputPartNoAtpIndex + 1;

  // work atp of send top node
  const int downTuppAtp = 1;       // work atp
  const int downTuppAtpIndex = 2;  // after consts and temps
  const int sendTopWorkTupps = downTuppAtpIndex + 1;

  // work atp of send bottom node
  const int returnTuppAtp = 1;       // work atp
  const int returnTuppAtpIndex = 2;  // after consts and temps
  const int sendBottomWorkTupps = returnTuppAtpIndex + 1;

  // work atp of split bottom node
  const int partInputTuppAtpIndex = 2;  // after consts and temps
  const int outputPartNoAtp = 1;        // work ATP
  const int outputPartNoAtpIndex = partInputTuppAtpIndex + 1;
  const int conversionErrorAtp = 1;  // work ATP
  const int conversionErrorAtpIndex = outputPartNoAtpIndex + 1;
  const int splitBottomWorkTupps = conversionErrorAtpIndex + 1;

  // child of split bottom node
  const int espChildAtp = 0;            // main ATP
  const int espChildInputAtpIndex = 2;  // after const and temp
  const int espChildPartInpAtpIndex = espChildInputAtpIndex + 1;
  const int espChildWorkTupps = espChildPartInpAtpIndex + 1;

  ex_cri_desc *given_cri_desc = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc *returned_cri_desc = new (space) ex_cri_desc(given_cri_desc->noTuples() + 1, space);

  ex_cri_desc *splitTopWorkCriDesc = new (space) ex_cri_desc(splitTopWorkTupps, space);
  ex_cri_desc *sendTopWorkCriDesc = new (space) ex_cri_desc(sendTopWorkTupps, space);
  ex_cri_desc *sendBottomWorkCriDesc;
  ex_cri_desc *splitBottomWorkCriDesc;
  ex_cri_desc *sendBottomDownCriDesc;
  ex_cri_desc *sendBottomUpCriDesc;
  ex_cri_desc *child_down_cri_desc;
  ex_cri_desc *child_up_cri_desc;

  // two value id lists describing the columns that are sent up and down
  // via IPC messages, the map tables describing the data sent, and the
  // length of the sent records
  ValueIdList sentInputValues;
  MapTable *initialESPMapTable = NULL;
  int downRecordLength;
  int downSqlBufferLength;
  int numDownBuffers = getDefault(GEN_SNDT_NUM_BUFFERS);
  CostScalar numRowsDown = (CostScalar)1;  // $$$$ get from input log props

  ValueIdList returnedOutputValues;
  MapTable *returnedValuesMapTable = NULL;
  int upRecordLength = 0;
  int upSqlBufferLength = 0;
  int numUpBuffers = getDefault(GEN_SNDB_NUM_BUFFERS);
  CostScalar numRowsUp = getEstRowsUsed().value();
  int mergeKeyLength = 0;

  const PartitioningFunction *topPartFunc = getTopPartitioningFunction();
  const PartitioningFunction *bottomPartFunc = getBottomPartitioningFunction();
  int numTopPartitions = topPartFunc->getCountOfPartitions();
  int numBottomPartitions = bottomPartFunc->getCountOfPartitions();
  int numTopEsps = numTopPartitions;
  int numBottomEsps = numBottomPartitions;
  NABoolean possiblePartNoConversionError = FALSE;

  // a value id list describing the values that the ESP needs to determine
  // the partition it is working on (part. number or key range) and its
  // corresponding map table
  const ValueIdList &partitionInputValues = getBottomPartitionInputValues();
  int partitionInputDataLength = 0;

  // the four TDBs that will be generated
  ComTdbSplitTop *splitTop = NULL;
  ComTdbSendTop *sendTop = NULL;
  ComTdbSplitBottom *splitBottom = NULL;
  ComTdbSendBottom *sendBottom = NULL;

  // expressions to be generated:
  //
  // - calculate input partition number (split top)
  // - encode merge key from a child into a key buffer so that
  //   a binary comparison can be done to merge rows
  // - move parent's input values into a contiguous buffer (send top)
  // - calculate output partition number (split bottom)
  // - move output values into contiguous buffer (send bottom)
  //
  ex_expr *calcInputPartNoExpr = NULL;
  ex_expr *mergeKeyExpr = NULL;
  ex_expr *inputMoveExpr = NULL;
  ex_expr *calcOutputPartNoExpr = NULL;
  ex_expr *outputMoveExpr = NULL;

  // helpers for skew buster
  //
  NABoolean useSkewBuster = FALSE;
  NABoolean broadcastSkew = FALSE;
  NABoolean broadcastOneRow = FALSE;
  int numSkewHashValues = 0;
  long *skewHashValues = NULL;
  SplitBottomSkewInfo *skewInfo = NULL;
  int initialRoundRobin = 0;
  int finalRoundRobin = numTopEsps - 1;

  generator->incrEspLevel();

  // If this is an extract producer query then manufacture a security key
  char extractSecurityKey[100];
  if (isExtractProducer_) {
    ComUID uid;
    uid.make_UID();
    long i64 = uid.get_value();
    str_sprintf(extractSecurityKey, "%ld", i64);
  } else {
    str_sprintf(extractSecurityKey, "0");
  }

  // Raise an error if this is a parallel extract consumer and the
  // child has a selection predicate.
  PhysicalExtractSource *extractConsumerChild = NULL;
  if (isExtractConsumer_) {
    GenAssert(child(0)->getOperatorType() == REL_EXTRACT_SOURCE, "Child of root must be REL_EXTRACT_SOURCE");
    extractConsumerChild = (PhysicalExtractSource *)(child(0).getPtr());
    if (extractConsumerChild->getSelectionPred().entries() != 0) {
      *CmpCommon::diags() << DgSqlCode(-7005);
      GenExit();
      return NULL;
    }
  }

  // ---------------------------------------------------------------------
  // make lists of values that go over the wire: input, output, and
  // partition input data
  // ---------------------------------------------------------------------

  // Take the characteristic inputs of the exchange node: these are the
  // input values that will travel down the wire. Decide on some sequence
  // in which to put them (choose value id order). Both split top and
  // split bottom node have to use the same sequence, of course.
  // Note that some additional input values that identify the actual
  // partition assigned to the split bottom node may exist and come from
  // another source. Those values are not added to the list.
  // This code decides the sequence of the values in the sent records.
  // Constants are not sent up or down in messages.
  for (ValueId x = getGroupAttr()->getCharacteristicInputs().init(); getGroupAttr()->getCharacteristicInputs().next(x);
       getGroupAttr()->getCharacteristicInputs().advance(x)) {
    if (x.getItemExpr()->getOperatorType() != ITM_CONSTANT AND NOT partitionInputValues.contains(x))
      sentInputValues.insert(x);
  }

  // Normally the outputs we send up are the non-constant values in
  // the characteristic outputs of the child.
  //
  // Exceptions to this rule:
  // - A parallel extract consumer returns all of the child's
  //   output columns.
  // - A parallel extract producer returns everything in the root's
  //   select list.
  //
  // Note that this code decides the sequence of the values in the
  // sent records.
  //
  if (isExtractConsumer_) {
    returnedOutputValues = extractConsumerChild->getTableDesc()->getColumnList();
  } else if (isExtractProducer_) {
    GenAssert(extractSelectList_, "Select list should not be NULL for a producer query");
    returnedOutputValues = *extractSelectList_;

    RETDesc *retDesc = generator->getBindWA()->getTopRoot()->getRETDesc();

    for (CollIndex i = 0; i < returnedOutputValues.entries(); i++) {
      ValueId val_id = returnedOutputValues[i];
      ItemExpr *item_expr = val_id.getItemExpr();
      if (!(val_id.getType() == retDesc->getType(i))) {
        item_expr = new (generator->wHeap()) Cast(item_expr, &(retDesc->getType(i)));
        item_expr->bindNode(generator->getBindWA());

        returnedOutputValues[i] = item_expr->getValueId();
      }
    }
  } else {
    for (ValueId y = child(0)->getGroupAttr()->getCharacteristicOutputs().init();
         child(0)->getGroupAttr()->getCharacteristicOutputs().next(y);
         child(0)->getGroupAttr()->getCharacteristicOutputs().advance(y)) {
      if (y.getItemExpr()->getOperatorType() != ITM_CONSTANT) returnedOutputValues.insert(y);
    }
  }

  // ---------------------------------------------------------------------
  // generate expressions to be evaluated in the client
  // ---------------------------------------------------------------------

  // generate expr to calculate input partition number
  if (FALSE /*  enable this later $$$$ */) {
    ItemExpr *bottomPartExpr = ((PartitioningFunction *)bottomPartFunc)->createPartitioningExpression();
    ValueIdList bottomPartList;
    int dummyLength;

    if (bottomPartExpr) {
      bottomPartList.insert(bottomPartExpr->getValueId());
      expGen->generateContiguousMoveExpr(bottomPartList, -1, inputPartNoAtp, inputPartNoAtpIndex,
                                         ExpTupleDesc::SQLARK_EXPLODED_FORMAT, dummyLength, &calcInputPartNoExpr);
    }
  }

  // expression to move input values into a single tuple
  expGen->generateContiguousMoveExpr(sentInputValues,
                                     -1,  // add convert nodes
                                     downTuppAtp, downTuppAtpIndex, ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                                     downRecordLength, &inputMoveExpr, 0, ExpTupleDesc::SHORT_FORMAT,
                                     &initialESPMapTable);

  // we MUST be able to fit at least one row into a buffer

  downSqlBufferLength =
      MAXOF((int)ComTdbSendTop::minSendBufferSize(downRecordLength), (int)(downMessageBufferLength_.getValue() * 1024));

  CollIndex childFragmentId = 0;

  NABoolean thisExchangeUsesSM = FALSE;
  if (thisExchangeCanUseSM(generator->getBindWA())) thisExchangeUsesSM = TRUE;

  int minMaxValueDownRecordLength = 0;

  if (isExtractConsumer_) {
    // Each extract consumer can change the format back to exploded if need be
    // to get the work distributed.
    // generator->setExplodedInternalFormat();
    ExpTupleDesc::TupleDataFormat tupleFormat = ExpTupleDesc::SQLARK_EXPLODED_FORMAT;

    // For an extract consumer we are going to bypass everything
    // related to the bottom fragment. All we need to do before
    // generating the top fragment (the split top and send top TDBs)
    // is add reply buffer output values to the map table.
    int dummyRecLen = 0;
    expGen->processValIdList(returnedOutputValues,        // [IN] ValueIdList
                             tupleFormat,                 // [IN] tuple data format
                             dummyRecLen,                 // [OUT] tuple length
                             0,                           // [IN] atp number
                             given_cri_desc->noTuples(),  // [IN] index into atp
                             NULL,                        // [optional OUT] tuple desc
                             ExpTupleDesc::SHORT_FORMAT   // [optional IN] tuple desc format
    );
  } else {
    // ---------------------------------------------------------------------
    // Generate expressions and TDBs to be executed in the ESP
    // ---------------------------------------------------------------------
    childFragmentId =
        generator->getFragmentDir()->pushFragment(FragmentDir::ESP, numBottomEsps, getBottomPartitioningFunction());

    if (CmpCommon::getDefault(ODBC_PROCESS) == DF_ON) {
      generator->getFragmentDir()->setAllEspFragmentsNeedTransaction();
    }

    space = generator->getSpace();

    // generate a small piece of space at the beginning of this fragment,
    // to get rid of the useless object that has offset 0 (an offset
    // of 0 is identified as a NULL object)
    space->allocateAlignedSpace(1);

    // save my own stack of map tables and start over from scratch
    // with a single map table for the child that describes the received
    // input values (the split bottom node will pass this tupp down
    // to its child)
    parentsSavedMapTable = generator->getMapTable();

    generator->setMapTable(initialESPMapTable);

    mapTable = generator->getMapTable();

    // the sent down values' attributes need to be changed now from
    // the work atp from which they are referenced to the actual atp
    // in which they are passed to the child of the split bottom node
    // which is the first tupp in the main atp (atp = 0, atpindex = 2)
    CollIndex oi = 0;
    for (oi = 0; oi < sentInputValues.entries(); oi++) {
      Attributes *attr = generator->getMapInfo(sentInputValues[oi])->getAttr();

      // the row coming up from the connection is returned as the tupp
      // following the input tupps
      attr->setAtp(espChildAtp);
      attr->setAtpIndex(espChildInputAtpIndex);
    }

    // ---------------------------------------------------------------------
    // set down cri desc for child
    // ---------------------------------------------------------------------
    sendBottomWorkCriDesc = new (space) ex_cri_desc(sendBottomWorkTupps, space);

    // splitBottomWorkCriDesc = new(space)
    //  ex_cri_desc(splitBottomWorkTupps,space);

    child_down_cri_desc = new (space) ex_cri_desc(espChildWorkTupps, space);
    generator->setCriDesc(child_down_cri_desc, Generator::DOWN);

    // allocate message data CRI descriptors for the server side
    sendBottomDownCriDesc = new (space) ex_cri_desc(3, space);

    // sendBottomDownCriDesc   = new(space) ex_cri_desc(given_cri_desc->noTuples(),space);

    // Normally the left down request is the same as the parent request
    // (givenDesc).  But if this HashJoin is doing the min max
    // optimization, then the left down request will contain an extra
    // tuple for the min and max values.  In this case, we will create a
    // new criDesc for the left down request and call it
    // splitBottomDownDesc.
    ex_cri_desc *splitBottomDownDesc = sendBottomDownCriDesc;

    // Assign attributes to the partition input values
    // such that they appear in a contiguous tuple
    if (NOT partitionInputValues.isEmpty()) {
      ((PartitioningFunction *)bottomPartFunc)
          ->generatePivLayout(generator, partitionInputDataLength, espChildAtp, espChildPartInpAtpIndex, NULL);

      generator->getFragmentDir()->setPartInputDataLength(childFragmentId, (int)partitionInputDataLength);
    }

    // Copy #BMOs value from Exchange node into the fragment
    generator->getFragmentDir()->setNumBMOs(childFragmentId, numBMOs_);
    generator->getFragmentDir()->setBMOsMemoryUsage(childFragmentId, BMOsMemoryUsage_.value());

    // store the numBottomEsps from this node for access by child sort operator.
    int saveNumEsps = generator->getNumESPs();
    generator->setNumESPs(numBottomEsps);

    generator->getFragmentDir()->setEspLevel(childFragmentId, generator->getEspLevel());

    // The atpIndex of the min/max tuple in the down queue ATP between
    // SendTop and SendBottom. This value must be 2 per the current
    // communication prototocal between the two operators.
    short minMaxValsAtpIndexSendTop2Bottom = 0;

    // The atpIndex of the min/max tuple (temp. result) in the workAtp.
    short minMaxValsWorkAtpIndex = 0;

    // Generate the min/max expression.
    ex_expr *minMaxExpr = 0;

    // Generate the min/max value move out expression.
    ex_expr *minMaxMoveOutExpr = 0;

    // A List to contain the Min and Max aggregates
    ValueIdList mmPairs;

    int minValStartOffset = 0;

    if (getScanFilterInputs().entries()) {
      minMaxValsAtpIndexSendTop2Bottom = downTuppAtpIndex;

      minMaxValsWorkAtpIndex = splitBottomWorkTupps;

      splitBottomWorkCriDesc = new (space) ex_cri_desc(minMaxValsWorkAtpIndex + 1, space);

      // We will be using the generated attributes for filter hostvars in
      // HashJoin to help define the filter expressions here in Exchange.
      // We even want to assign the computed filter expressions back to the
      // same filter host vars.

      Attributes *mapAttr = NULL;

      NABoolean startOffsetTaken = FALSE;

      UInt32 inputs = getScanFilterInputs().entries();
      // Fetch one unit from the generator
      for (CollIndex i = 0; i < inputs; i++) {
        const ScanFilterInput &input = getScanFilterInput(i);

        ValueId minVal = input.getMinVar();
        ValueId maxVal = input.getMaxVar();
        ValueId rangeValueVal = input.getRangeOfValuesVar();

        ItemExpr *minValExprSB = new (generator->wHeap()) Aggregate(ITM_MIN, minVal.getItemExpr(), FALSE);

        ItemExpr *maxValExprSB = new (generator->wHeap()) Aggregate(ITM_MAX, maxVal.getItemExpr(), FALSE);

        ItemExpr *rangeValueExprSB =
            new (generator->wHeap()) Aggregate(ITM_RANGE_VALUES_MERGE, rangeValueVal.getItemExpr(), FALSE);

        Int16 filterId = rangeValueVal.getItemExpr()->getFilterId();

        ((Aggregate *)(rangeValueExprSB))->setFilterId(filterId);

        minValExprSB->bindNode(generator->getBindWA());
        maxValExprSB->bindNode(generator->getBindWA());
        rangeValueExprSB->bindNode(generator->getBindWA());

        ValueId minValExprSBId = minValExprSB->getValueId();
        ValueId maxValExprSBId = maxValExprSB->getValueId();
        ValueId rangeValueExprSBId = rangeValueExprSB->getValueId();

        // Map the min aggregate to be the same as the min placeholder
        // (system generated hostvar).  Set the ATP/ATPIndex to refer to
        // the min/max tuple in the workAtp
        mapAttr = (generator->getMapInfo(minVal))->getAttr();

        if (!startOffsetTaken) {
          minValStartOffset = mapAttr->getNullIndOffset();
          startOffsetTaken = TRUE;
        }

        mapAttr = (generator->addMapInfo(minValExprSBId, mapAttr))->getAttr();
        mapAttr->setAtp(outputPartNoAtp);  // workAtp
        mapAttr->setAtpIndex(minMaxValsWorkAtpIndex);

        // Map the max aggregate to be the same as the max placeholder
        // (system generated hostvar).
        mapAttr = (generator->getMapInfo(maxVal))->getAttr();

        mapAttr = (generator->addMapInfo(maxValExprSBId, mapAttr))->getAttr();
        mapAttr->setAtp(outputPartNoAtp);  // workAtp
        mapAttr->setAtpIndex(minMaxValsWorkAtpIndex);

        mmPairs.insert(minValExprSBId);
        mmPairs.insert(maxValExprSBId);

        // Map the range aggregate to be the same as the range placeholder
        // (a system generated hostvar).
        if (generator->isMinmaxOptWithRangeOfValues() && generator->getMapInfoAsIs(rangeValueVal)) {
          mapAttr = (generator->getMapInfo(rangeValueVal))->getAttr();

          mapAttr = (generator->addMapInfo(rangeValueExprSBId, mapAttr))->getAttr();
          mapAttr->setAtp(outputPartNoAtp);  // workAtp
          mapAttr->setAtpIndex(minMaxValsWorkAtpIndex);

          mmPairs.insert(rangeValueExprSBId);
        }

        // record the min/max val list in this for
        // explain
        this->setScanFilterExpressions(mmPairs);
      }

      expGen->generateAggrExpr(mmPairs, ex_expr::exp_AGGR, &minMaxExpr, 0, true);

      // We also need to generate a move expression to copy the final min/max
      // values back into the down queue entry at
      // { downTuppAtp, downTuppAtpIndex }. The starting offset of the value
      // is generator->getMapInfo(minValExprSBId))->getAttr()).getNullIndOffset().
      //
      // Need to temp. set the tuple to 0 for the location of the min/max
      // data as source. See ex_split_bottom.cpp,  minMaxMoveOutExpr_->eval(),
      // where the workAtp_ is the first argument (0-indexed).
      //
      // We also need to prepare a list of Attributes (called offsets) to use
      // when generating the move out expression. The offsets (null/varchar/data)
      // in these attributes are for the filter hostvars, and will be the
      // target offsets for the move (aka memcpy()) operations. In this way,
      // we assign the merged filters back to the input tupp.

      Attributes **offsets = new (space) Attributes *[mmPairs.entries()];

      for (CollIndex i = 0; i < mmPairs.entries(); i++) {
        mapAttr = (generator->getMapInfo(mmPairs[i]))->getAttr();
        mapAttr->setAtp(0);

        offsets[i] = mapAttr;
      }

      ExpTupleDesc *minMaxTupleDescriptor = NULL;

      // generator->reportMappingInfo(cout, mmPairs, "Exchange, before gen mmMove:");

      expGen->generateContiguousMoveExpr(mmPairs,  // The source of the computed
                                                   // min of min and max of max
                                                   // is at { 0, minMaxValsWorkAtpIndex  }
                                                   // as arranged in the eval() call
                                                   // in ex_split_bottom_tcb::work().
                                         -1,       // add convert nodes
                                         downTuppAtp, downTuppAtpIndex, ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                                         minMaxValueDownRecordLength, &minMaxMoveOutExpr,
                                         &minMaxTupleDescriptor,          /* targetValues */
                                         ExpTupleDesc::LONG_FORMAT, NULL, /* newMapTable*/
                                         NULL,                            /* ValueIdList *tgtValues  */
                                         0,                               // IN, start offset
                                         NULL,                            // IN(O)
                                         FALSE,                           // IN
                                         NULL,                            // IN
                                         FALSE,                           // IN, disable bulk move
                                         offsets                          // IN, offsets to use
      );

      // delete only the array itself, not attribute* in
      // each element.
      NADELETEBASIC(offsets, space);

      // Save the descriptor of the result in workAtp's desc. This is useful
      // to dump the min/max values per their data type.
      splitBottomWorkCriDesc->setTupleDescriptor(minMaxValsWorkAtpIndex, minMaxTupleDescriptor);

      // Restore the ATP back to outputPartNoAtp
      for (CollIndex i = 0; i < mmPairs.entries(); i++) {
        mapAttr = (generator->getMapInfo(mmPairs[i]))->getAttr();
        mapAttr->setAtp(outputPartNoAtp);
      }

      // Reset the perform min/max opt flag to assure that the same information
      // will not not be used by another exchange node.
      // generator->setPerformMinMaxOptForType1(FALSE);

    }  // end of handling min/max for type-1
    else {
      splitBottomWorkCriDesc = new (space) ex_cri_desc(splitBottomWorkTupps, space);
    }

    // ---------------------------------------------------------------------
    // generate child tree
    // ---------------------------------------------------------------------
    child(0)->codeGen(generator);

    generator->setNumESPs(saveNumEsps);

    ComTdb *child_tdb = (ComTdb *)(generator->getGenObj());
    ExplainTuple *childExplainTuple = generator->getExplainTuple();
    child_up_cri_desc = generator->getCriDesc(Generator::UP);

    // split bottom passes up queue entries unchanged to send bottom node
    // (never mind the partition input values that travel with it, they
    // don't get moved into the message buffers)
    sendBottomUpCriDesc = child_up_cri_desc;

    // ---------------------------------------------------------------------
    // generate expressions for split bottom node
    // ---------------------------------------------------------------------

    // generate expr to calculate output partition number
    if ((numTopEsps > 1) && (NOT isAnESPAccess())) {
      UInt32 expectedPartInfoLength = sizeof(int);

      ItemExpr *topPartExpr = topPartFunc->getPartitioningExpression();

      // A ReplicationPartitioningFunction has no partitioning expression.
      if (topPartExpr) {
        ValueIdList topPartExprAsList;
        int partNoValLength;
        ItemExpr *convErrExpr = topPartFunc->getConvErrorExpr();

        if (convErrExpr) {
          // There is a "Narrow" operator in the partitioning expression
          // and the split bottom TCB will have to handle cases of
          // errors during a data conversion. Add an entry to the
          // map table that assigns convErrExpr the right location
          // (ATP=conversionErrorAtp, ATPIndex=conversionErrorAtpIndex,
          // Offset=0)
          possiblePartNoConversionError = TRUE;

          ValueId convErrorValId = convErrExpr->getValueId();
          // should this use a separate map table?
          Attributes *convAttr = generator->addMapInfo(convErrorValId, NULL)->getAttr();
          convAttr->setAtp(conversionErrorAtp);
          convAttr->setAtpIndex(conversionErrorAtpIndex);
          convAttr->setOffset(0);
          convAttr->setTupleFormat(ExpTupleDesc::SQLARK_EXPLODED_FORMAT);
        }

        if (topPartExpr->getOperatorType() == ITM_HASH2_DISTRIB &&
            topPartExpr->child(0)->getOperatorType() == ITM_HDPHASH &&
            topPartExpr->child(0)->child(0)->getOperatorType() == ITM_NARROW &&
            topPartExpr->child(0)->child(0)->child(0)->getOperatorType() == ITM_RANDOMNUM &&
            getDefaultAsLong(USE_ROUND_ROBIN_FOR_RANDOMNUM) > 1) {
          RandomNum *randomNum = static_cast<RandomNum *>(topPartExpr->child(0)->child(0)->child(0).getPtr());

          if (randomNum->isSimple()) {
            // Replace "hash2(RandomNum for p)" with
            // "RandomNum mod p", since this gives a smoother
            // distribution, especially given that simple
            // RandomNum values are sequential values.
            // USE_ROUND_ROBIN_FOR_RANDOMNUM values:
            //
            // CQD value  transform hash2 to mod  use thread id in RandomNum
            // ---------  ----------------------  --------------------------
            //     0               no                          no
            //     1               no                          yes
            //     2               yes                         no
            //     3               yes                         yes

            topPartExpr = new (generator->wHeap())
                Modulus(randomNum, new (generator->wHeap())
                                       ConstValue(topPartFunc->getCountOfPartitions(), (NAMemory *)generator->wHeap()));
            topPartExpr->synthTypeAndValueId();
            // remember it for EXPLAIN
            overridePartExpr_ = topPartExpr;
          }
        }

        // executor has hard-coded assumption that the result is an unsigned
        // long.
        // Add a Cast node to convert result to an unsigned long.

        ItemExpr *cast = new (generator->wHeap()) Cast(
            topPartExpr, new (generator->wHeap()) SQLInt(generator->wHeap(), FALSE,
                                                         topPartExpr->getValueId().getType().supportsSQLnullLogical()));

        cast->bindNode(generator->getBindWA());

        topPartExprAsList.insert(cast->getValueId());

        const SkewedDataPartitioningFunction *skpf = topPartFunc->castToSkewedDataPartitioningFunction();

        // Process getSkewProperty.
        skewProperty::skewDataHandlingEnum skew = (skpf) ? skpf->getSkewProperty().getIndicator() : skewProperty::ANY;

        if (skpf AND(skew == skewProperty::BROADCAST || skew == skewProperty::UNIFORM_DISTRIBUTE)) {
          // 1. Add the hashing expression (to return hash value
          //    only) to the topPartExprAsList.

          ItemExpr *hashExpr = topPartFunc->getHashingExpression();

          GenAssert(hashExpr != NULL, "getHashingExpression returned NULL");

          ItemExpr *hashExprResult = new (generator->wHeap())
              Cast(hashExpr, new (generator->wHeap()) SQLLargeInt(generator->wHeap(), TRUE,  // allow negative values.
                                                                  FALSE                      // no nulls.
                                                                  ));

          hashExprResult->bindNode(generator->getBindWA());
          topPartExprAsList.insert(hashExprResult->getValueId());

          expectedPartInfoLength += sizeof(long)  // hash value
                                    + 4;          // alignment

          // 2. Prepare the array of hash values which
          //    indicate possible skewed partitioning keys.

#if 0
              // Please note that I never reserved COMP_BOOL_154 
              // and just added this for my private build. 
              if (CmpCommon::getDefault(COMP_BOOL_154) == DF_ON)
                {
                  // Test limit of 10000.  Don't care that they
                  // are actual skewed values.

                  numSkewHashValues = 10000;     
                  skewHashValues = new (space) long[numSkewHashValues];
                  for (int sv = 0; sv < numSkewHashValues; sv++)
                    skewHashValues[sv] = (long) sv;
                  skewInfo = new (space) 
                      SplitBottomSkewInfo(numSkewHashValues, skewHashValues);
                }
              else
#else
          {
            Int64List *partFuncSkewedValues =
                ((SkewedDataPartitioningFunction *)topPartFunc)->buildHashListForSkewedValues();

            GenAssert(partFuncSkewedValues != NULL, "NULL returned from buildHashListForSkewedValues");

            numSkewHashValues = partFuncSkewedValues->entries();

            GenAssert(numSkewHashValues > 0, "buildHashListForSkewedValues returned zero or fewer values");

            skewHashValues = new (space) long[numSkewHashValues];

            for (int sv = 0; sv < numSkewHashValues; sv++) skewHashValues[sv] = (*partFuncSkewedValues)[sv];

            skewInfo = new (space) SplitBottomSkewInfo(numSkewHashValues, skewHashValues);
          }
#endif

          // 3. Prepare the skew properties for ComTdbSplitBottom.

          useSkewBuster = TRUE;
          broadcastOneRow = FALSE;
          if (skew == skewProperty::BROADCAST) {
            broadcastSkew = TRUE;
            broadcastOneRow = skpf->getSkewProperty().getBroadcastOneRow();
          } else
            broadcastSkew = FALSE;
          srand((UInt32)JULIANTIMESTAMP(3));
          initialRoundRobin = rand() % numTopEsps;
          int antiSkewESPs = skpf->getSkewProperty().getAntiSkewESPs();
          if (antiSkewESPs <= 0) {
            // For hash join, make sure all consumer ESPs get the
            // the skewed value rows.
            antiSkewESPs = numTopEsps;
          } else {
            // For NJ OCR skewbuster, just use the CQD.
          }

          finalRoundRobin = (initialRoundRobin + antiSkewESPs - 1) % numTopEsps;
        }

        expGen->generateContiguousMoveExpr(topPartExprAsList,
                                           0,  // don't add convert node
                                           outputPartNoAtp, outputPartNoAtpIndex, ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                                           partNoValLength, &calcOutputPartNoExpr);
        GenAssert(partNoValLength == expectedPartInfoLength, "Unexpected length of result of part. function.");
      } else {
        GenAssert(topPartFunc->isAReplicationPartitioningFunction(),
                  "Did not create part. expr. for repartitioning function");
      }
    }

    if (isAnESPAccess()) {
      GenAssert((calcOutputPartNoExpr == NULL), "ESP ACCESS node must not have calcOutputPartNoExpr");
    }

    // If this is a top level ESP then switch from internal format to
    // exploded format here in the ESP rather than having to do the switch
    // of data formats in the master for all ESPs.
    ExpTupleDesc::TupleDataFormat tupleFormat = generator->getInternalFormat();
    NABoolean resizeCifRecord = FALSE;
    NABoolean considerBufferDefrag = FALSE;
    NABoolean bmo_affinity = (CmpCommon::getDefault(COMPRESSED_INTERNAL_FORMAT_BMO_AFFINITY) == DF_ON);
    if (!bmo_affinity && getCachedTupleFormat() != ExpTupleDesc::UNINITIALIZED_FORMAT &&
        CmpCommon::getDefault(COMPRESSED_INTERNAL_FORMAT) == DF_SYSTEM &&
        CmpCommon::getDefault(COMPRESSED_INTERNAL_FORMAT_BMO) == DF_SYSTEM) {
      resizeCifRecord = getCachedResizeCIFRecord();
      tupleFormat = getCachedTupleFormat();
      considerBufferDefrag = getCachedDefrag() && resizeCifRecord;
    } else {
      tupleFormat = determineInternalFormat(returnedOutputValues, this, resizeCifRecord, generator, bmo_affinity,
                                            considerBufferDefrag);
      considerBufferDefrag = considerBufferDefrag && resizeCifRecord;
    }

    if ((CmpCommon::getDefault(COMPRESSED_INTERNAL_FORMAT_ROOT_DOES_CONVERSION) == DF_OFF ||
         getExtractProducerFlag()) &&  // if extract producer convert to exploded format
        generator->isCompressedInternalFormat() &&
        isParentRoot()) {
      tupleFormat = ExpTupleDesc::SQLARK_EXPLODED_FORMAT;
      resizeCifRecord = FALSE;
      considerBufferDefrag = FALSE;
    }

    // move J output tupps to a contiguous buffer (have to move even if
    // the output already has a single tupp, since we have to allocate the
    // target tupp inside a message buffer)
    expGen->generateContiguousMoveExpr(returnedOutputValues,
                                       -1,  // add convert nodes
                                       returnTuppAtp, returnTuppAtpIndex, tupleFormat, upRecordLength, &outputMoveExpr,
                                       NULL, ExpTupleDesc::SHORT_FORMAT, &returnedValuesMapTable);

    // we MUST be able to fit at least one row into a buffer
    upSqlBufferLength = MAXOF((int)ComTdbSendTop::minReceiveBufferSize(upRecordLength),
                              (int)(upMessageBufferLength_.getValue() * 1024));

    // ---------------------------------------------------------------------
    // generate send bottom tdb
    // ---------------------------------------------------------------------

    sendBottom = new (space) ComTdbSendBottom(
        outputMoveExpr, (queue_index)getDefault(GEN_SNDB_SIZE_DOWN), (queue_index)getDefault(GEN_SNDB_SIZE_UP),
        sendBottomDownCriDesc, sendBottomUpCriDesc, sendBottomWorkCriDesc, returnTuppAtpIndex, downRecordLength,
        upRecordLength, downSqlBufferLength, numDownBuffers, upSqlBufferLength, numUpBuffers,
        (Cardinality)numRowsDown.value(), (Cardinality)numRowsUp.value());

    sendBottom->setRecordLength(upRecordLength);

    sendBottom->setConsiderBufferDefrag(considerBufferDefrag);
    sendBottom->setCIFON((tupleFormat == ExpTupleDesc::SQLMX_ALIGNED_FORMAT));
    // ---------------------------------------------------------------------
    // generate split bottom tdb
    // ---------------------------------------------------------------------

    // handle min/max for type-1 HJ here.
    // The length of the min max tuple used by the min/max optimization.

    splitBottom = new (space) ComTdbSplitBottom(
        child_tdb, sendBottom, calcOutputPartNoExpr, outputPartNoAtpIndex, possiblePartNoConversionError,
        conversionErrorAtpIndex, partInputTuppAtpIndex, partitionInputDataLength,
        (Cardinality)getGroupAttr()->getOutputLogPropList()[0]->getResultCardinality().value(), sendBottomDownCriDesc,
        sendBottomUpCriDesc, splitBottomWorkCriDesc, TRUE, numTopEsps, numTopPartitions, numBottomEsps,
        numBottomPartitions, skewInfo,
        minMaxValsWorkAtpIndex,       // The min/max tupp index in workAtp
        minMaxValueDownRecordLength,  // the length of the min/max tupp
        minValStartOffset,            // the start offset for the min/max values
                                      // in workAtp. Needed for allocating extra
                                      // space with.
        minMaxExpr, minMaxMoveOutExpr);

    // reset the min max row length since this
    // splitBottom has taken care of the min/max
    // (all filters) accumualted so far (on the
    // path to this exhcange).
    // generator->setMinMaxRowLength(0);

    // It is important that we initialize splitBottom first
    // and then sendBottom to get the tdbId
    splitBottom->setRecordLength(getGroupAttr()->getRecordLength());
    generator->initTdbFields(splitBottom);
    generator->initTdbFields(sendBottom);

    splitBottom->setUseSkewBuster(useSkewBuster);
    splitBottom->setBroadcastSkew(broadcastSkew);

    splitBottom->setInitialRoundRobin(initialRoundRobin);
    splitBottom->setFinalRoundRobin(finalRoundRobin);
    splitBottom->setBroadcastOneRow(broadcastOneRow);

    splitBottom->setCIFON((tupleFormat == ExpTupleDesc::SQLMX_ALIGNED_FORMAT));

    if (generator->parquetInSupport()) splitBottom->setParquetInSupport(TRUE);

    if (CmpCommon::getDefault(COMP_BOOL_153) == DF_ON) splitBottom->setForceSkewRoundRobin(TRUE);

    splitBottom->setAbendType((int)CmpCommon::getDefaultNumeric(COMP_INT_39));
    double cpuLimitCheckFreq = CmpCommon::getDefaultNumeric(COMP_INT_48);
    if (cpuLimitCheckFreq > SHRT_MAX) cpuLimitCheckFreq = SHRT_MAX;
    splitBottom->setCpuLimitCheckFreq((int)cpuLimitCheckFreq);

    // tell the tdb whether we collect statistics or not
    if (generator->computeStats()) {
      splitBottom->setCollectStats(generator->computeStats());
      splitBottom->setCollectStatsType(generator->collectStatsType());
      splitBottom->setCollectRtsStats(generator->collectRtsStats());
    }

    // Set overflow mode. Needed for accumulated stats.
    splitBottom->setOverflowMode(generator->getOverflowMode());

    // Config query execution limits.
    int cpuLimit = (int)CmpCommon::getDefaultNumeric(QUERY_LIMIT_SQL_PROCESS_CPU);
    if (cpuLimit > 0) splitBottom->setCpuLimit(cpuLimit);

    if (CmpCommon::getDefault(QUERY_LIMIT_SQL_PROCESS_CPU_DEBUG) == DF_ON) splitBottom->setQueryLimitDebug();

    // There are two SeaMonster flags in split bottom
    // * Whether SM is used somewhere in the query
    // * Whether SM is used in this exchange
    //
    // Send top, send bottom, and split top only carry the exchange
    // flag not the query-level flag
    if (generator->getQueryUsesSM()) splitBottom->setQueryUsesSM();

    if (thisExchangeUsesSM) {
      splitBottom->setExchangeUsesSM();
      sendBottom->setExchangeUsesSM();
    }

    if (isExtractProducer_) {
      // Set a flag in the split bottom and send bottom TDBs
      splitBottom->setExtractProducerFlag();
      sendBottom->setExtractProducerFlag();

      // Create a copy of the security key and give the split bottom a
      // pointer to it
      char *keyCopy = space->allocateAndCopyToAlignedSpace(extractSecurityKey, str_len(extractSecurityKey), 0);

      ComExtractProducerInfo *producerInfo = new (space) ComExtractProducerInfo();

      producerInfo->setSecurityKey(keyCopy);
      splitBottom->setExtractProducerInfo(producerInfo);
    }

    if (hash2RepartitioningWithSameKey()) splitBottom->setMWayRepartitionFlag();

    if (isAnESPAccess()) {
      splitBottom->setMWayRepartitionFlag();
      splitBottom->setIsAnESPAccess();
      generator->getFragmentDir()->setSoloFragment(childFragmentId, TRUE);
    }

    // Compute memory estimate for SendBottom
    // -----------------------------------------------------------------------------------------
    double totalMemorySB = numRowsUp.value() * upRecordLength + numRowsDown.value() * downRecordLength;
    // divide by 2 to get an everage values since actual number of up buffers
    // varies at run time based on the rate at which rows are consumed and
    // traffic at a given a SendBottom.  In bytes
    const double memoryLimitPerCpuSB = (numUpBuffers * upSqlBufferLength) / 2 + (numDownBuffers * downSqlBufferLength);

    if (bottomPartFunc->isAReplicationPartitioningFunction() == TRUE) {
      totalMemorySB *= numBottomEsps;
    }
    double memoryPerCpuSB = totalMemorySB / numBottomEsps;
    if (memoryPerCpuSB > memoryLimitPerCpuSB) memoryPerCpuSB = memoryLimitPerCpuSB;
    totalMemorySB = memoryPerCpuSB * numBottomEsps;

    // Compute memory estimate for SendTop
    // -----------------------------------------------------------------------------------------
    double totalMemoryST = numRowsUp.value() * upRecordLength + numRowsDown.value() * downRecordLength;
    // divide by 2 to get an everage values since actual number of up buffers
    // varies at run time based on the rate at which rows are consumed and
    // traffic at a given a SendBottom.  In bytes
    const double memoryLimitPerCpuST = (numUpBuffers * upSqlBufferLength) / 2 + (numDownBuffers * downSqlBufferLength);

    if (topPartFunc->isAReplicationPartitioningFunction() == TRUE) {
      totalMemoryST *= numTopEsps;
    }
    double memoryPerCpuST = totalMemoryST / numTopEsps;
    if (memoryPerCpuST > memoryLimitPerCpuST) memoryPerCpuST = memoryLimitPerCpuST;
    totalMemoryST = memoryPerCpuST * numTopEsps;

    generator->addToTotalEstimatedMemory(totalMemoryST + totalMemorySB);

    if (!generator->explainDisabled()) {
      int sbMemEstInKBPerNode = (int)((totalMemoryST + totalMemorySB) / 1024);
      sbMemEstInKBPerNode = sbMemEstInKBPerNode / (MAXOF(generator->compilerStatsInfo().dop(), 1));

      generator->setExplainTuple(addExplainInfo(splitBottom, childExplainTuple, 0, generator));
      sendBottom->setExplainNodeId(generator->getExplainNodeId());
    }

    // ExplainTuple *sendBotExplain =
    //  addExplainInfo(sendBottom, splitBotExplain, 0, generator);

    generator->getFragmentDir()->setTopObj((char *)splitBottom);

    // ---------------------------------------------------------------------
    // return back to the original fragment, the server fragment has
    // been generated
    // ---------------------------------------------------------------------
    generator->getFragmentDir()->popFragment();
    space = generator->getSpace();

    // get rid of the child's map tables, restore the old view
    generator->removeAll();

    generator->setMapTable(parentsSavedMapTable);
    mapTable = generator->getMapTable();

    // the returned values are available to the parent node
    generator->appendAtEnd(returnedValuesMapTable);

    // the returned values' attributes need to be changed now from
    // the work atp from which they are referenced to the actual atp
    // in which they are returned to the parent of the send top node
    // (atp = 0, atpindex = # of input tupps)
    for (oi = 0; oi < returnedOutputValues.entries(); oi++) {
      Attributes *attr =
          generator->getMapInfoFromThis(generator->getLastMapTable(), returnedOutputValues[oi])->getAttr();

      // the row coming up from the connection is returned as the tupp
      // following the input tupps
      attr->setAtp(0);
      attr->setAtpIndex(given_cri_desc->noTuples());
    }

    // ---------------------------------------------------------------------
    // Generate comparison expression for merging sorted streams
    // ---------------------------------------------------------------------

    // generate merge key expression. If this is an extract
    // producer query, there is no need for merging because data rows
    // are going to be routed to alternate masters.
    // Also, if this esp change was added during PreCodeGen for halloween
    // protection, ignore the merge, because otherwise we get deadlocks
    // -- see solution 10-081023-6759.
    if (!isExtractProducer_ && !forcedHalloweenProtection_ && sortKeyForMyOutput_.entries() > 0) {
      ValueIdList encodeList;

      for (CollIndex i = 0; i < sortKeyForMyOutput_.entries(); i++) {
        ItemExpr *ix = sortKeyForMyOutput_[i].getItemExpr();

        if (ix->getOperatorType() == ITM_INVERSE)
          ix = new (generator->wHeap()) CompEncode(ix->child(0).getPtr(), TRUE);
        else
          ix = new (generator->wHeap()) CompEncode(ix, FALSE);

        ix->synthTypeAndValueId();
        encodeList.insert(ix->getValueId());
      }

      expGen->generateContiguousMoveExpr(encodeList,
                                         0,  // don't add convert nodes,
                                         mergeTuppAtp, mergeTuppAtpIndex, ExpTupleDesc::SQLMX_KEY_FORMAT,
                                         mergeKeyLength, &mergeKeyExpr);
    }

  }  // if (isExtractConsumer) else ...

  // ---------------------------------------------------------------------
  // Generate send top tdb
  // ---------------------------------------------------------------------
  //

  sendTop = new (space)
      ComTdbSendTop(childFragmentId, inputMoveExpr, given_cri_desc, returned_cri_desc, NULL,
                    NULL,  // get rid of these later
                    sendTopWorkCriDesc, downTuppAtpIndex, (queue_index)getDefault(GEN_SNDT_SIZE_DOWN),
                    (queue_index)getDefault(GEN_SNDT_SIZE_UP), downRecordLength, upRecordLength, downSqlBufferLength,
                    numDownBuffers, upSqlBufferLength, numUpBuffers, (Cardinality)numRowsDown.value(),
                    (Cardinality)numRowsUp.value(), (CmpCommon::getDefault(EXE_DIAGNOSTIC_EVENTS) == DF_ON));

  sendTop->setRecordLength(upRecordLength);
  generator->initTdbFields(sendTop);

  if (isExtractConsumer_) {
    // Set a flag in the send top TDB
    sendTop->setExtractConsumerFlag();

    // Create a copy of the phandle string and the security key and
    // give the TDB pointers to them
    const NAString &espForExtract = extractConsumerChild->getEspPhandle();
    const NAString &securityKey = extractConsumerChild->getSecurityKey();
    const char *espData = espForExtract.data();
    const char *keyData = securityKey.data();
    char *espCopy = space->allocateAndCopyToAlignedSpace(espData, str_len(espData), 0);
    char *keyCopy = space->allocateAndCopyToAlignedSpace(keyData, str_len(keyData), 0);

    ComExtractConsumerInfo *consumerInfo = new (space) ComExtractConsumerInfo();

    consumerInfo->setEspPhandle(espCopy);
    consumerInfo->setSecurityKey(keyCopy);
    sendTop->setExtractConsumerInfo(consumerInfo);
  }

  // Allow the fix for soln 10-100508-0135 to be undone
  if (CmpCommon::getDefault(COMP_BOOL_118) == DF_ON) sendTop->setUseOldStatsNoWaitDepth();

  // Set the flag that tells send top TCB whether to restrict the
  // number of send buffers to 1. By default the CQD is ON and the
  // flag is set.
  if (CmpCommon::getDefault(GEN_SNDT_RESTRICT_SEND_BUFFERS) == DF_ON) sendTop->setRestrictSendBuffers();

  // ---------------------------------------------------------------------
  // Create a split top node, to be executed in the current process
  // ---------------------------------------------------------------------

  splitTop = new (space) ComTdbSplitTop(
      sendTop, calcInputPartNoExpr, inputPartNoAtpIndex, mergeKeyExpr, mergeTuppAtpIndex, (int)mergeKeyLength, NULL, -1,
      -1, given_cri_desc, returned_cri_desc, given_cri_desc, splitTopWorkCriDesc, FALSE, (queue_index)2,
      (queue_index)getDefault(GEN_SPLT_SIZE_UP),
      (Cardinality)getGroupAttr()->getOutputLogPropList()[0]->getResultCardinality().value(), numBottomPartitions,
      CmpCommon::getDefaultNumeric(STREAM_TIMEOUT), getDefault(GEN_SID_NUM_BUFFERS), getDefault(GEN_SID_BUFFER_SIZE));

  splitTop->setRecordLength(getGroupAttr()->getRecordLength());

  generator->initTdbFields(splitTop);

  if (generator->isLRUOperation()) splitTop->setLRUOperation();

  if (thisExchangeUsesSM) {
    splitTop->setExchangeUsesSM();
    sendTop->setExchangeUsesSM();
  }

  if (isExtractProducer_) {
    // Set a flag in the split top and send top TDBs
    splitTop->setExtractProducerFlag();
    sendTop->setExtractProducerFlag();

    // Create a copy of the security key and give the split top a
    // pointer to it
    char *keyCopy = space->allocateAndCopyToAlignedSpace(extractSecurityKey, str_len(extractSecurityKey), 0);

    ComExtractProducerInfo *producerInfo = new (space) ComExtractProducerInfo();

    producerInfo->setSecurityKey(keyCopy);
    splitTop->setExtractProducerInfo(producerInfo);
  }

  if (isExtractConsumer_) splitTop->setExtractConsumerFlag();

  if (hash2RepartitioningWithSameKey()) splitTop->setMWayRepartitionFlag();

  if (!generator->explainDisabled()) {
    if (isExtractConsumer_)
      generator->setExplainTuple(addExplainInfo(sendTop, 0, 0, generator));
    else
      sendTop->setExplainNodeId(generator->getExplainNodeId());
    splitTop->setExplainNodeId(generator->getExplainNodeId());
  }

  // Assign a SeaMonster tag to send top and send bottom. Both TDBs
  // use the same tag. When EXPLAIN is enabled the tag can be the
  // EXPLAIN ID. Otherwise we use an integer unique within this plan.
  if (thisExchangeUsesSM) {
    int smTag = generator->getExplainNodeId();
    if (generator->explainDisabled()) smTag = generator->getNextSMTag();
    sendTop->setSMTag(smTag);
    sendBottom->setSMTag(smTag);
  }

  if (isAnESPAccess()) {
    splitTop->setMWayRepartitionFlag();
  }

  generator->decrEspLevel();

  // ---------------------------------------------------------------------
  // setup everything and leave
  // ---------------------------------------------------------------------
  generator->setCriDesc(given_cri_desc, Generator::DOWN);
  generator->setCriDesc(returned_cri_desc, Generator::UP);
  generator->setGenObj(this, splitTop);

  return 0;
}

ExpTupleDesc::TupleDataFormat Exchange::determineInternalFormat(const ValueIdList &valIdList, RelExpr *relExpr,
                                                                NABoolean &resizeCifRecord, Generator *generator,
                                                                NABoolean bmo_affinity,
                                                                NABoolean &considerBufferDefrag) {
  RelExpr::CifUseOptions bmo_cif = RelExpr::CIF_SYSTEM;

  if (CmpCommon::getDefault(COMPRESSED_INTERNAL_FORMAT_BMO) == DF_OFF) {
    bmo_cif = RelExpr::CIF_OFF;
  } else if (CmpCommon::getDefault(COMPRESSED_INTERNAL_FORMAT_BMO) == DF_ON) {
    bmo_cif = RelExpr::CIF_ON;
  }

  // CIF_SYSTEM

  return generator->determineInternalFormat(valIdList, relExpr, resizeCifRecord, bmo_cif, bmo_affinity,
                                            considerBufferDefrag);
}
CostScalar Exchange::getEstimatedRunTimeMemoryUsage(Generator *generator, NABoolean perNode, int *numStreams) {
  //////////////////////////////////////
  // compute the buffer length (for both
  // sendTop and sendBottom) first.
  //////////////////////////////////////
  int upRowLength = getGroupAttr()->getCharacteristicOutputs().getRowLength();
  int downRowLength = getGroupAttr()->getCharacteristicInputs().getRowLength();

  // make sure the up buffer can fit at least one row
  int upSqlBufferLength =
      MAXOF((int)ComTdbSendTop::minReceiveBufferSize(upRowLength), (int)(upMessageBufferLength_.getValue() * 1024));

  // make sure the down buffer can fit at least one row
  int downSqlBufferLength =
      MAXOF((int)ComTdbSendTop::minSendBufferSize(downRowLength), (int)(downMessageBufferLength_.getValue() * 1024));

  int sqlBufferLengthUsed = MAXOF(upSqlBufferLength, downSqlBufferLength);

  //////////////////////////////////////
  // compute the number of buffers
  //////////////////////////////////////
  int numUpBuffersSendT = getDefault(GEN_SNDT_NUM_BUFFERS);
  int numUpBuffersSendB = getDefault(GEN_SNDB_NUM_BUFFERS);

  int numDownBuffersSendT = 1;  // only one down buffer allocated
  int numDownBuffersSendB = 1;  // only one down buffer allocated

  double memoryRequired = 0;

  const PartitioningFunction *topPartFunc = getTopPartitioningFunction();
  int numTopEsps = topPartFunc->getCountOfPartitions();

  if (isDP2Exchange() == FALSE) {
    // regular ESP exchange

    // Compute for send top first.

    // Average it out because the memory for upper queue
    // is allocated dynamically
    double topMemory =
        (sqlBufferLengthUsed + 1000) * numUpBuffersSendT / 2 + (sqlBufferLengthUsed + 1000) * numDownBuffersSendT;

    if (topPartFunc->isAReplicationPartitioningFunction() == TRUE) {
      topMemory *= numTopEsps;
    }

    memoryRequired = topMemory;

    // Compute for send bottom
    double bottomMemory =
        (sqlBufferLengthUsed + 1000) * numUpBuffersSendB / 2 + (sqlBufferLengthUsed + 1000) * numDownBuffersSendB;

    memoryRequired += bottomMemory;

  } else {
    // split top.
  }

  if (numStreams != NULL) *numStreams = numTopEsps;
  if (perNode)
    memoryRequired /= MINOF(MAXOF(CURRCONTEXT_CLUSTERINFO->getTotalNumberOfCPUs(), 1), numTopEsps);
  else
    memoryRequired /= numTopEsps;
  return memoryRequired;
}

double Exchange::getEstimatedRunTimeMemoryUsage(Generator *generator, ComTdb *tdb) {
  int numOfStreams = 1;
  CostScalar totalMemory = getEstimatedRunTimeMemoryUsage(generator, FALSE, &numOfStreams);
  totalMemory = totalMemory * numOfStreams;
  return totalMemory.value();
}

bool Exchange::thisExchangeCanUseSM(BindWA *bindWA) const {
  // SeaMonster can be enabled if all the following are true
  // * The SEAMONSTER default is ON or the env var SQ_SEAMONSTER is 1
  // * This is an ESP exchange
  // * This is not a parallel extract producer or consumer
  // * This is not an ESP access operator

  if (isEspExchange() && !isExtractProducer_ && !isExtractConsumer_ && !isAnESPAccess() &&
      bindWA->queryCanUseSeaMonster()) {
    return true;
  }

  return false;
}
