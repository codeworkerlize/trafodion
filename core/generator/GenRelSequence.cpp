
#include "optimizer/GroupAttr.h"
#include "optimizer/AllRelExpr.h"
#include "optimizer/RelSequence.h"
#include "generator/Generator.h"
#include "GenExpGenerator.h"
#include "exp/ExpCriDesc.h"
#include "comexe/ComTdbSequence.h"
#include "common/NumericType.h"
#include "optimizer/ItmFlowControlFunction.h"
#include "comexe/ExplainTupleMaster.h"
#include "common/ComDefs.h"

#include "comexe/HashBufferHeader.h"

ItemExpr *addConvNode(ItemExpr *childExpr, ValueIdMap *mapping, CollHeap *wHeap)

{
  if (childExpr->getOperatorType() != ITM_CONVERT && !childExpr->isASequenceFunction()) {
    ValueId topValue;
    mapping->mapValueIdUp(topValue, childExpr->getValueId());
    if (topValue == childExpr->getValueId()) {
      // add the convert node
      ItemExpr *newChild = new (wHeap) Convert(childExpr);
      newChild->synthTypeAndValueId(TRUE);
      mapping->addMapEntry(newChild->getValueId(), childExpr->getValueId());
      return newChild;
    } else {
      return topValue.getItemExpr();
    }
  }
  return childExpr;
}

// getHistoryAttributes
//
// Helper function that traverses the set of root sequence functions
// supplied by the compiler and constructs the set of all of the
// attributes that must be materialized in the history row.
//
void PhysSequence::getHistoryAttributes(const ValueIdSet &sequenceFunctions, const ValueIdSet &outputFromChild,
                                        ValueIdSet &historyAttributes, NABoolean addConvNodes, CollHeap *wHeap,
                                        ValueIdMap *origAttributes) const {
  if (addConvNodes && !origAttributes) {
    origAttributes = new (wHeap) ValueIdMap();
  }

  ValueIdSet children;
  for (ValueId valId = sequenceFunctions.init(); sequenceFunctions.next(valId); sequenceFunctions.advance(valId)) {
    if (valId.getItemExpr()->isASequenceFunction()) {
      ItemExpr *itmExpr = valId.getItemExpr();

      switch (itmExpr->getOperatorType()) {
          // The child needs to be in the history row only during reading phase.
          // Fall through.
        case ITM_OLAP_LEAD:
        case ITM_OLAP_LAG:
        case ITM_OFFSET:
        case ITM_ROWS_SINCE:
        case ITM_THIS:
        case ITM_NOT_THIS:
        case ITM_OLAP_FIRST_VALUE:
        case ITM_OLAP_LAST_VALUE:
        case ITM_OLAP_PIVOT_GROUP:
          // If the child needs to be in the history buffer, then
          // add a Convert node to force the value to be moved to the
          // history buffer.
          if (addConvNodes) {
            itmExpr->child(0) = addConvNode(itmExpr->child(0), origAttributes, wHeap);
          }
          historyAttributes += itmExpr->child(0)->getValueId();
          break;

          // The sequence function needs to be in the history row.
          //
        case ITM_RUNNING_SUM:
        case ITM_RUNNING_COUNT:
        case ITM_RUNNING_MIN:
        case ITM_RUNNING_MAX:
        case ITM_LAST_NOT_NULL:
        case ITM_RUNNING_PIVOT_GROUP:
          historyAttributes += itmExpr->getValueId();
          break;
          /*
                  // after PhysSequence precode gen OLAP sum and count are already transform,ed into running
                  // this is used during optimization phase--
                  case ITM_OLAP_SUM:
                  case ITM_OLAP_COUNT:
                  case ITM_OLAP_RANK:
                  case ITM_OLAP_DRANK:
                    if (addConvNodes)
                      {
                        itmExpr->child(0) =
                          addConvNode(itmExpr->child(0), origAttributes, wHeap);
                      }

                    historyAttributes += itmExpr->child(0)->getValueId();
                    //historyAttributes += itmExpr->getValueId();
                    break;
          */
          // The child and sequence function need to be in the history row.
          //
        case ITM_OLAP_MIN:
        case ITM_OLAP_MAX:
        case ITM_MOVING_MIN:
        case ITM_MOVING_MAX:

          // If the child needs to be in the history buffer, then
          // add a Convert node to force the value to be moved to the
          // history buffer.
          if (addConvNodes) {
            itmExpr->child(0) = addConvNode(itmExpr->child(0), origAttributes, wHeap);
          }

          historyAttributes += itmExpr->child(0)->getValueId();
          historyAttributes += itmExpr->getValueId();
          break;

        case ITM_RUNNING_CHANGE:
          if (itmExpr->child(0)->getOperatorType() == ITM_ITEM_LIST) {
            // child is a multi-valued expression
            //
            ExprValueId treePtr = itmExpr->child(0);

            ItemExprTreeAsList changeValues(&treePtr, ITM_ITEM_LIST, RIGHT_LINEAR_TREE);

            CollIndex nc = changeValues.entries();

            ItemExpr *newChild = NULL;
            if (addConvNodes) {
              newChild = addConvNode(changeValues[nc - 1], origAttributes, wHeap);
              historyAttributes += newChild->getValueId();
            } else {
              historyAttributes += changeValues[nc - 1]->getValueId();
            }

            // add each item in the list
            //
            for (CollIndex i = nc; i > 0; i--) {
              if (addConvNodes) {
                ItemExpr *conv = addConvNode(changeValues[i - 1], origAttributes, wHeap);

                newChild = new (wHeap) ItemList(conv, newChild);
                newChild->synthTypeAndValueId(TRUE);
                historyAttributes += conv->getValueId();
              } else {
                historyAttributes += changeValues[i - 1]->getValueId();
              }
            }

            if (addConvNodes) {
              itmExpr->child(0) = newChild;
            }
          } else {
            // If the child needs to be in the history buffer, then
            // add a Convert node to force the value to be moved to the
            // history buffer.
            if (addConvNodes) {
              itmExpr->child(0) = addConvNode(itmExpr->child(0), origAttributes, wHeap);
            }

            historyAttributes += itmExpr->child(0)->getValueId();
          }

          historyAttributes += itmExpr->getValueId();
          break;

        default:
          CMPASSERT(0);
      }
    }

    // Gather all the children, and if not empty, recurse down to the
    // next level of the tree.
    //
    for (int i = 0; i < valId.getItemExpr()->getArity(); i++) {
      if (!outputFromChild.contains(valId.getItemExpr()->child(i)->getValueId()))
      //! valId.getItemExpr()->child(i)->nodeIsPreCodeGenned())
      {
        children += valId.getItemExpr()->child(i)->getValueId();
      }
    }
  }

  if (NOT children.isEmpty()) {
    getHistoryAttributes(children, outputFromChild, historyAttributes, addConvNodes, wHeap, origAttributes);
  }

}  // PhysSequence::getHistoryAttributes

void PhysSequence::processHistoryAttributesForLead(ValueIdSet &readFunctions, ValueIdSet &returnFunctions,
                                                   ValueIdSet &historyAttributes, CollHeap *wHeap) {
  ValueIdSet leadFunctions;
  ValueIdSet leadFuncChildren;

  readFunctions.findAllOpType(ITM_OLAP_LEAD, leadFunctions);

  for (ValueId valId = leadFunctions.init(); leadFunctions.next(valId); leadFunctions.advance(valId)) {
    ItemExpr *itmExpr = valId.getItemExpr();
    ItmLeadOlapFunction *lead = (ItmLeadOlapFunction *)itmExpr;

    leadFuncChildren += lead->child(0)->getValueId();

    // offset expression: must be casted to SQL INT
    if (lead->getArity() > 1) {
      const NAType &cType = lead->child(1)->getValueId().getType();

      // (must be) signed; nulls allowed (if allowed by child1)
      ItemExpr *castExpr =
          new (wHeap) Cast(lead->child(1), new (wHeap) SQLInt(wHeap, TRUE, cType.supportsSQLnullLogical()));
      castExpr->synthTypeAndValueId(TRUE);
      lead->child(1) = castExpr;

      leadFuncChildren += castExpr->getValueId();
    }

    // handle the default expression (child(2)) if exists.
    // The type of the default value expression must be that
    // of child(0).
    if (lead->getArity() > 2) {
      const NAType &cType = lead->child(0)->getValueId().getType();

      // (must be) signed; nulls allowed (if allowed by child1)
      ItemExpr *castExpr = new (wHeap) Cast(lead->child(2), cType.newCopy(wHeap));
      castExpr->synthTypeAndValueId(TRUE);

      lead->child(2) = castExpr;

      leadFuncChildren += castExpr->getValueId();
    }
  }

  readFunctions -= leadFunctions;
  readFunctions += leadFuncChildren;

  historyAttributes += readFunctions;

  // I do not think the old and the new form of the LEAD functions are
  // different. The following code is retained anyway.
  //
  // remove LEAD in old form from the return function
  ValueIdSet leadFuncsInReturn;
  returnFunctions.findAllOpType(ITM_OLAP_LEAD, leadFuncsInReturn);
  returnFunctions -= leadFuncsInReturn;

  // add in the LEAD in new form
  returnFunctions += leadFunctions;
}

// PhysSequence::computeHistoryAttributes
//
// Helper function to compute the attribute for the history buffer based
// on the items projected from the child and the computed history items.
// Also, adds the attribute information the the map table.
//
void PhysSequence::computeHistoryAttributes(Generator *generator, MapTable *localMapTable, Attributes **attrs,
                                            const ValueIdSet &historyIds) const {
  // Get a local handle on some of the generator objects.
  //
  CollHeap *wHeap = generator->wHeap();

  // Populate the attribute vector with the flattened list of sequence
  // functions and/or sequence function arguments that must be in the
  // history row. Add convert nodes for the items that are not sequence
  // functions to force them to be moved into the history row.
  //
  if (NOT historyIds.isEmpty()) {
    int i = 0;
    ValueId valId;

    for (valId = historyIds.init(); historyIds.next(valId); historyIds.advance(valId)) {
      // If this is not a sequence function, then insert a convert
      // node.
      //
      if (!valId.getItemExpr()->isASequenceFunction()) {
        // Get a handle on the original expression and erase
        // the value ID.
        //
        ItemExpr *origExpr = valId.getItemExpr();
        origExpr->setValueId(NULL_VALUE_ID);
        origExpr->markAsUnBound();

        // Construct the cast expression with the original expression
        // as the child -- must have undone the child value ID to
        // avoid recursion later.
        //
        ItemExpr *castExpr = new (wHeap) Cast(origExpr, &(valId.getType()));

        // Replace the expression for the original value ID and the
        // synthesize the types and value ID for the new expression.
        //
        valId.replaceItemExpr(castExpr);
        castExpr->synthTypeAndValueId(TRUE);
      }
      attrs[i++] = (generator->addMapInfoToThis(localMapTable, valId, 0))->getAttr();
    }
  }
}  // PhysSequence::computeHistoryAttributes

// computeHistoryBuffer
//
// Helper function that traverses the set of root sequence functions
// supplied by the compiler and dynamically determines the size
// of the history buffer.
//
void PhysSequence::computeHistoryRows(const ValueIdSet &sequenceFunctions,  // historyIds
                                      int &computedHistoryRows, int &unableToCalculate,
                                      NABoolean &unboundedFollowing, int &minFollowingRows,
                                      const ValueIdSet &outputFromChild) {
  ValueIdSet children;
  ValueIdSet historyAttributes;
  int value = 0;

  for (ValueId valId = sequenceFunctions.init(); sequenceFunctions.next(valId); sequenceFunctions.advance(valId)) {
    if (valId.getItemExpr()->isASequenceFunction()) {
      ItemExpr *itmExpr = valId.getItemExpr();

      switch (itmExpr->getOperatorType()) {
        // THIS and NOT THIS are not dynamically computed
        //
        case ITM_THIS:
        case ITM_NOT_THIS:
          break;

        // The RUNNING functions and LastNotNull all need to go back just one row.
        //
        case ITM_RUNNING_SUM:
        case ITM_RUNNING_COUNT:
        case ITM_RUNNING_MIN:
        case ITM_RUNNING_MAX:
        case ITM_RUNNING_CHANGE:
        case ITM_LAST_NOT_NULL:
        case ITM_RUNNING_PIVOT_GROUP:
          computedHistoryRows = MAXOF(computedHistoryRows, 2);
          break;

        case ITM_OLAP_LEAD:
          value = ((ItmLeadOlapFunction *)itmExpr)->getOffset();

        /// set to unable to compute for now-- will change later to compte values from frameStart_ and frameEnd_
        case ITM_OLAP_SUM:
        case ITM_OLAP_COUNT:
        case ITM_OLAP_MIN:
        case ITM_OLAP_MAX:
        case ITM_OLAP_RANK:
        case ITM_OLAP_DRANK:
        case ITM_OLAP_FIRST_VALUE:
        case ITM_OLAP_LAST_VALUE:
        case ITM_OLAP_PIVOT_GROUP: {
          if (!outputFromChild.contains(itmExpr->getValueId())) {
            ItmSeqOlapFunction *olap = (ItmSeqOlapFunction *)itmExpr;

            if (olap->isFrameStartUnboundedPreceding())  //(olap->getframeStart() == - INT_MAX)
            {
              computedHistoryRows = MAXOF(computedHistoryRows, 2);
            } else {
              computedHistoryRows = MAXOF(computedHistoryRows, ABS(olap->getframeStart()) + 2);
            }
            if (!olap->isFrameEndUnboundedFollowing())  //(olap->getframeEnd() != INT_MAX)
            {
              computedHistoryRows = MAXOF(computedHistoryRows, ABS(olap->getframeEnd()) + 1);
            }

            if (olap->isFrameEndUnboundedFollowing())  //(olap->getframeEnd() == INT_MAX)
            {
              unboundedFollowing = TRUE;
              if (olap->getframeStart() > 0) {
                minFollowingRows =
                    ((minFollowingRows > olap->getframeStart()) ? minFollowingRows : olap->getframeStart());
              }
            } else if (olap->getframeEnd() > 0) {
              minFollowingRows = ((minFollowingRows > olap->getframeEnd()) ? minFollowingRows : olap->getframeEnd());
            }
          }
        }

        break;

        // If 'rows since', we cannot determine how much history is needed.
        case ITM_ROWS_SINCE:
          unableToCalculate = 1;
          break;

          // The MOVING and  OFFSET functions need to go back as far as the value
          // of their second child.
          //
          //  The second argument can be:
          //    Constant: for these, we can use the constant value to set the upper bound
          //              for the history buffer.
          //    ItmScalarMinMax(child0, child1) (with operType = ITM_SCALAR_MIN)
          //      - if child0 or child1 is a constant, then we can use either one
          //        to set the upper bound.

        case ITM_MOVING_MIN:
        case ITM_MOVING_MAX:
        case ITM_OFFSET:
        case ITM_OLAP_LAG:
          for (int i = 1; i < itmExpr->getArity(); i++) {
            if (itmExpr->child(i)->getOperatorType() != ITM_NOTCOVERED) {
              ItemExpr *exprPtr = itmExpr->child(i);
              NABoolean negate;
              ConstValue *cv = exprPtr->castToConstValue(negate);
              if (cv AND cv->canGetExactNumericValue()) {
                int scale;
                long value64 = cv->getExactNumericValue(scale);

                if (scale == 0 && value64 >= 0 && value64 < INT_MAX) {
                  value64 = (negate ? -value64 : value64);
                  value = MAXOF((int)value64, value);
                }
              } else {
                if (exprPtr->getOperatorType() == ITM_SCALAR_MIN) {
                  for (int j = 0; j < exprPtr->getArity(); j++) {
                    if (exprPtr->child(j)->getOperatorType() != ITM_NOTCOVERED) {
                      ItemExpr *exprPtr1 = exprPtr->child(j);
                      NABoolean negate1;
                      ConstValue *cv1 = exprPtr1->castToConstValue(negate1);
                      if (cv1 AND cv1->canGetExactNumericValue()) {
                        int scale1;
                        long value64_1 = cv1->getExactNumericValue(scale1);

                        if (scale1 == 0 && value64_1 >= 0 && value64_1 < INT_MAX) {
                          value64_1 = (negate1 ? -value64_1 : value64_1);
                          value = MAXOF((int)value64_1, value);
                        }
                      }
                    }
                  }
                }
              }  // end of inner else
            }    // end of if

          }  // end of for

          // Check if the value is greater than zero.
          // If it is, then save the value, but first
          // increment the returned ConstValue by one.
          // Otherwise, the offset or moving value was unable
          // to be calculated.

          if (value > 0) {
            value++;
            computedHistoryRows = MAXOF(computedHistoryRows, value);
            value = 0;
          } else
            unableToCalculate = 1;

          break;

        default:
          CMPASSERT(0);
      }
    }

    // Gather all the children, and if not empty, recurse down to the
    // next level of the tree.
    //

    for (int i = 0; i < valId.getItemExpr()->getArity(); i++) {
      if (  // valId.getItemExpr()->child(i)->getOperatorType() != ITM_NOTCOVERED //old stuff
          !outputFromChild.contains(valId.getItemExpr()->child(i)->getValueId())) {
        children += valId.getItemExpr()->child(i)->getValueId();
      }
    }
  }

  if (NOT children.isEmpty()) {
    computeHistoryRows(children, computedHistoryRows, unableToCalculate, unboundedFollowing, minFollowingRows,
                       outputFromChild);
  }
}  // PhysSequence::computeHistoryRows

// PhysSequence::generateChildProjectExpression
//
// Helper function to compute the child project expression. Also, sets up the
// local map table so that the moved items will be referenced from their new
// location. Returns the resulting project expression.
//
#ifdef OLD
ex_expr *PhysSequence::generateChildProjectExpression(Generator *generator, MapTable *mapTable, MapTable *localMapTable,
                                                      const ValueIdSet &childProjectIds) const {
  ex_expr *projectExpr = NULL;
  if (NOT childProjectIds.isEmpty()) {
    // Generate the clauses for the expression
    //
    generator->getExpGenerator()->generateSetExpr(childProjectIds, ex_expr::exp_ARITH_EXPR, &projectExpr);

    // Add the projected values to the local map table.
    //
    ValueId valId;
    for (valId = childProjectIds.init(); childProjectIds.next(valId); childProjectIds.advance(valId)) {
      // Get the attribute information from the convert destination.
      //
      Attributes *newAttr = mapTable->getMapInfo(valId)->getAttr();

      // Add the original value to the local map table with the
      // attribute information from the convert desination.
      //
      MapInfo *mapInfo = localMapTable->addMapInfoToThis(valId.getItemExpr()->child(0)->getValueId(), newAttr);

      // Nothing more needs to be done for this item.
      //
      mapInfo->codeGenerated();
    }
  }

  return projectExpr;
}  // PhysSequence::generateChildProjectExpression
#endif

void RelSequence::addCancelExpr(CollHeap *wHeap) {
  ItemExpr *cPred = NULL;

  if (this->partition().entries() > 0) {
    return;
  }
  if (cancelExpr().entries() > 0) {
    return;
  }

  for (ValueId valId = selectionPred().init(); selectionPred().next(valId); selectionPred().advance(valId)) {
    ItemExpr *pred = valId.getItemExpr();

    // Look for preds that select a prefix of the sequence.
    // Rank() < const; Rank <= const; const > Rank; const >= Rank
    ItemExpr *op1 = NULL;
    ItemExpr *op2 = NULL;

    if (pred->getOperatorType() == ITM_LESS || pred->getOperatorType() == ITM_LESS_EQ) {
      op1 = pred->child(0);
      op2 = pred->child(1);
    } else if (pred->getOperatorType() == ITM_GREATER || pred->getOperatorType() == ITM_GREATER_EQ) {
      op1 = pred->child(1);
      op2 = pred->child(0);
    }
    NABoolean negate;
    if (op1 && op2 && (op2->getOperatorType() == ITM_CONSTANT || op2->getOperatorType() == ITM_DYN_PARAM) &&
        (op1->getOperatorType() == ITM_OLAP_RANK || op1->getOperatorType() == ITM_OLAP_DRANK ||
         (op1->getOperatorType() == ITM_OLAP_COUNT && op1->child(0)->getOperatorType() == ITM_CONSTANT &&
          !op1->child(0)->castToConstValue(negate)->isNull()))) {
      cPred = new (wHeap) UnLogic(ITM_NOT, pred);
      // break at first occurence
      break;
    }
  }

  if (cPred) {
    cPred->synthTypeAndValueId(TRUE);
    cancelExpr().insert(cPred->getValueId());
  }
}

void PhysSequence::addCheckPartitionChangeExpr(Generator *generator, NABoolean addConvNodes,
                                               ValueIdMap *origAttributes) {
  if (!(partition().entries() > 0)) {
    return;
  }
  CollHeap *wHeap = generator->wHeap();

  if (addConvNodes && !origAttributes) {
    origAttributes = new (wHeap) ValueIdMap();
  }

  ItemExpr *checkPartChng = NULL;
  for (CollIndex ix = 0; ix < partition().entries(); ix++) {
    ItemExpr *iePtr = partition().at(ix).getItemExpr();
    ItemExpr *convIePtr = addConvNode(iePtr, origAttributes, wHeap);

    movePartIdsExpr() += convIePtr->getValueId();
    ItemExpr *comp = new (wHeap) BiRelat(ITM_EQUAL, iePtr, convIePtr, TRUE);
    if (!checkPartChng) {
      checkPartChng = comp;
    } else {
      checkPartChng = new (wHeap) BiLogic(ITM_AND, checkPartChng, comp);
    }
  }

  checkPartChng->convertToValueIdSet(checkPartitionChangeExpr(), generator->getBindWA(), ITM_AND);
}

// PhysSequence::preCodeGen() -------------------------------------------
// Perform local query rewrites such as for the creation and
// population of intermediate tables, for accessing partitioned
// data. Rewrite the value expressions after minimizing the dataflow
// using the transitive closure of equality predicates.
//
// PhysSequence::preCodeGen() - is basically the same as the RelExpr::
// preCodeGen() except that here we replace the VEG references in the
// sortKey() list, as well as the selectionPred().
//
// Parameters:
//
// Generator *generator
//    IN/OUT : A pointer to the generator object which contains the state,
//             and tools (e.g. expression generator) to generate code for
//             this node.
//
// ValueIdSet &externalInputs
//    IN    : The set of external Inputs available to this node.
//
//
RelExpr *PhysSequence::preCodeGen(Generator *generator, const ValueIdSet &externalInputs, ValueIdSet &pulledNewInputs) {
  if (nodeIsPreCodeGenned()) return this;

  // Resolve the VEGReferences and VEGPredicates, if any, that appear
  // in the Characteristic Inputs, in terms of the externalInputs.
  //
  getGroupAttr()->resolveCharacteristicInputs(externalInputs);

  // My Characteristic Inputs become the external inputs for my children.
  //
  ValueIdSet childPulledInputs;

  child(0) = child(0)->preCodeGen(generator, externalInputs, pulledNewInputs);
  if (!child(0).getPtr()) return NULL;

  // process additional input value ids the child wants
  getGroupAttr()->addCharacteristicInputs(childPulledInputs);
  pulledNewInputs += childPulledInputs;

  // The VEG expressions in the selection predicates and the characteristic
  // outputs can reference any expression that is either a potential output
  // or a characteristic input for this RelExpr. Supply these values for
  // rewriting the VEG expressions.
  //
  ValueIdSet availableValues;
  getInputValuesFromParentAndChildren(availableValues);

  // The sequenceFunctions() have access to only the Input
  // Values. These can come from the parent or be the outputs of the
  // child.
  //
  sequenceFunctions().replaceVEGExpressions(availableValues, getGroupAttr()->getCharacteristicInputs());

  sequencedColumns().replaceVEGExpressions(availableValues, getGroupAttr()->getCharacteristicInputs());

  requiredOrder().replaceVEGExpressions(availableValues, getGroupAttr()->getCharacteristicInputs());

  cancelExpr().replaceVEGExpressions(availableValues, getGroupAttr()->getCharacteristicInputs());

  partition().replaceVEGExpressions(availableValues, getGroupAttr()->getCharacteristicInputs());

  // checkPartitionChangeExpr().
  // replaceVEGExpressions(availableValues,
  //                       getGroupAttr()->getCharacteristicInputs());

  // The selectionPred has access to only the output values generated by
  // Sequence and input values from the parent.
  //
  getInputAndPotentialOutputValues(availableValues);

  // Rewrite the selection predicates.
  //
  NABoolean replicatePredicates = TRUE;
  selectionPred().replaceVEGExpressions(availableValues, getGroupAttr()->getCharacteristicInputs(),
                                        FALSE,  // no key predicates here
                                        0 /* no need for idempotence here */, replicatePredicates);

  // Replace VEG references in the outputs and remove redundant
  // outputs.
  //
  getGroupAttr()->resolveCharacteristicOutputs(availableValues, getGroupAttr()->getCharacteristicInputs());

  addCancelExpr(generator->wHeap());

  /// addCheckPartitionChangeExpr(generator->wHeap());

  transformOlapFunctions(generator->wHeap());
  if (getUnboundedFollowing()) {
    // Count this Seq as a BMO and add its needed memory to the total needed
    generator->incrNumBMOs();

    if ((ActiveSchemaDB()->getDefaults()).getAsDouble(BMO_MEMORY_LIMIT_PER_NODE_IN_MB) > 0)
      generator->incrBMOsMemory(getEstimatedRunTimeMemoryUsage(generator, TRUE));
  } else
    generator->incrNBMOsMemoryPerNode(getEstimatedRunTimeMemoryUsage(generator, TRUE));

  markAsPreCodeGenned();

  return this;
}  // PhysSequence::preCodeGen

short PhysSequence::codeGen(Generator *generator) {
  // Get a local handle on some of the generator objects.
  //
  CollHeap *wHeap = generator->wHeap();
  Space *space = generator->getSpace();
  ExpGenerator *expGen = generator->getExpGenerator();
  MapTable *mapTable = generator->getMapTable();

  // Allocate a new map table for this node. This must be done
  // before generating the code for my child so that this local
  // map table will be sandwiched between the map tables already
  // generated and the map tables generated by my offspring.
  //
  // Only the items available as output from this node will
  // be put in the local map table. Before exiting this function, all of
  // my offsprings map tables will be removed. Thus, none of the outputs
  // from nodes below this node will be visible to nodes above it except
  // those placed in the local map table and those that already exist in
  // my ancestors map tables. This is the standard mechanism used in the
  // generator for managing the access to item expressions.
  //
  MapTable *localMapTable = generator->appendAtEnd();

  // Since this operation doesn't modify the row on the way down the tree,
  // go ahead and generate the child subtree. Capture the given composite row
  // descriptor and the child's returned TDB and composite row descriptor.
  //
  ex_cri_desc *givenCriDesc = generator->getCriDesc(Generator::DOWN);
  child(0)->codeGen(generator);
  ComTdb *childTdb = (ComTdb *)generator->getGenObj();
  ex_cri_desc *childCriDesc = generator->getCriDesc(Generator::UP);
  ExplainTuple *childExplainTuple = generator->getExplainTuple();

  // Make all of my child's outputs map to ATP 1. The child row is only
  // accessed in the project expression and it will be the second ATP
  // (ATP 1) passed to this expression.
  //
  localMapTable->setAllAtp(1);

  // My returned composite row has an additional tupp.
  //
  int numberTuples = givenCriDesc->noTuples() + 1;
  ex_cri_desc *returnCriDesc = new (space) ex_cri_desc(numberTuples, space);

  // For now, the history buffer row looks just the return row. Later,
  // it may be useful to add an additional tupp for sequence function
  // itermediates that are not needed above this node -- thus, this
  // ATP is kept separate from the returned ATP.
  //
  const int historyAtp = 0;
  const int historyAtpIndex = numberTuples - 1;
  ex_cri_desc *historyCriDesc = new (space) ex_cri_desc(numberTuples, space);
  ExpTupleDesc *historyDesc = 0;

  // seperate the read and retur expressions
  seperateReadAndReturnItems(wHeap);

  // The history buffer consists of items projected directly from the
  // child, the root sequence functions, the value arguments of the
  // offset functions, and running sequence functions. These elements must
  // be materialized in the  history buffer in order to be able to compute
  // the outputs of this node -- the items projected directly from the child
  // (projectValues) and the root sequence functions (sequenceFunctions).
  //
  // Compute the set of sequence function items that must be materialized
  // int the history buffer. -- sequenceItems
  //
  // Compute the set of items in the history buffer: the union of the
  // projected values and the value arguments. -- historyIds
  //
  // Compute the set of items in the history buffer that are computed:
  // the difference between all the elements in the history buffer
  // and the projected items. -- computedHistoryIds
  //

  // KB---will need to return atp with 3 tups only 0,1 and 2
  // 2 -->values from history buffer after ther are moved to it

  addCheckPartitionChangeExpr(generator, TRUE);

  ValueIdSet historyIds;

  historyIds += movePartIdsExpr();
  historyIds += sequencedColumns();

  ValueIdSet outputFromChild = child(0)->getGroupAttr()->getCharacteristicOutputs();

  getHistoryAttributes(readSeqFunctions(), outputFromChild, historyIds, TRUE, wHeap);

  // Process the read function, the return function  and historyId
  // for the 2nd and 3rd child of the LEAD. The frst child has been
  // processed in getHistoryAttributes() already.
  processHistoryAttributesForLead(readSeqFunctions(), returnSeqFunctions(), historyIds, wHeap);

  ValueIdSet lastValue;
  ValueIdSet lastValueChildren;
  readSeqFunctions().findAllOpType(ITM_OLAP_LAST_VALUE, lastValue);
  lastValue.findAllChildren(lastValueChildren);
  readSeqFunctions() -= lastValue;
  readSeqFunctions() += lastValueChildren;
  returnSeqFunctions() += lastValue;
  // Add in the top level sequence functions.
  historyIds += readSeqFunctions();

  // The usefulness of the following logic is to TBD
  //  if ( !leadFuncs.isEmpty() ) {
  //     ValueIdSet valSet;
  //     valSet += movePartIdsExpr();
  //     valSet += sequencedColumns();
  //
  //     ValueIdSet result;
  //     leadFuncs.addOlapLeadFuncs(valSet, result);
  //     returnSeqFunctions() += result;
  //  }

  // add counter column to history buffer
  // the counter column stores row number of current row.
  ValueIdSet firstValue;
  readSeqFunctions().findAllOpType(ITM_OLAP_FIRST_VALUE, firstValue);
  for (ValueId valId = firstValue.init(); firstValue.next(valId); firstValue.advance(valId)) {
    ItmOlapFirstLastValue *expr = (ItmOlapFirstLastValue *)valId.getItemExpr();
    historyIds += expr->counterVid();
  }

  getHistoryAttributes(returnSeqFunctions(), outputFromChild, historyIds, TRUE, wHeap);

  // Add in the top level functions.
  historyIds += returnSeqFunctions();

  // Layout the work tuple format which consists of the projected
  // columns and the computed sequence functions. First, compute
  // the number of attributes in the tuple.
  //
  int numberAttributes = ((NOT historyIds.isEmpty()) ? historyIds.entries() : 0);

  // Allocate an attribute pointer vector from the working heap.
  //
  Attributes **attrs = new (wHeap) Attributes *[numberAttributes];

  // Fill in the attributes vector for the history buffer including
  // adding the entries to the map table. Also, compute the value ID
  // set for the elements to project from the child row.
  //
  //??????????re-visit this function??
  computeHistoryAttributes(generator, localMapTable, attrs, historyIds);

  // Create the tuple descriptor for the history buffer row and
  // assign the offsets to the attributes. For now, this layout is
  // identical to the returned row. Set the tuple descriptors for
  // the return and history rows.
  //
  int historyRecLen;
  expGen->processAttributes(numberAttributes, attrs, ExpTupleDesc::SQLARK_EXPLODED_FORMAT, historyRecLen, historyAtp,
                            historyAtpIndex, &historyDesc, ExpTupleDesc::SHORT_FORMAT);
  NADELETEBASIC(attrs, wHeap);
  returnCriDesc->setTupleDescriptor(historyAtpIndex, historyDesc);
  historyCriDesc->setTupleDescriptor(historyAtpIndex, historyDesc);

  // If there are any sequence function items, generate the sequence
  // function expressions.
  //
  ex_expr *readSeqExpr = NULL;
  if (NOT readSeqFunctions().isEmpty()) {
    ValueIdSet seqVals = readSeqFunctions();
    seqVals += sequencedColumns();
    seqVals += movePartIdsExpr();

    // For some reason, PCODE does not compute a correct
    // result for the query (mantis 6746).
    //
    //  select first_value(ORG_LEVEL4_ID) over() from BBMIS_TOP_ORG;
    //
    // Before the root cause can be found, we conditionally disable
    // PCODE for the read function expression when
    //  1) There exists FIRST_VALUE() in the read function;
    //  2) The sequence column set is empty.
    UInt16 pcm = expGen->getPCodeMode();

    if (firstValue.entries() > 0 && sequencedColumns().entries() == 0) expGen->setPCodeMode(ex_expr::PCODE_NONE);

    expGen->generateSequenceExpression(seqVals, readSeqExpr);

    expGen->setPCodeMode(pcm);
  }

  ex_expr *checkPartChangeExpr = NULL;
  if (!checkPartitionChangeExpr().isEmpty()) {
    ItemExpr *newCheckPartitionChangeTree = checkPartitionChangeExpr().rebuildExprTree(ITM_AND, TRUE, TRUE);

    expGen->generateExpr(newCheckPartitionChangeTree->getValueId(), ex_expr::exp_SCAN_PRED, &checkPartChangeExpr);
  }

  // unsigned long rowLength;
  ex_expr *returnExpr = NULL;
  if (NOT returnSeqFunctions().isEmpty()) {
    expGen->generateSequenceExpression(returnSeqFunctions(), returnExpr);
  }

  // Generate expression to evaluate predicate on the output
  //
  ex_expr *postPred = 0;

  if (!selectionPred().isEmpty()) {
    ItemExpr *newPredTree = selectionPred().rebuildExprTree(ITM_AND, TRUE, TRUE);

    expGen->generateExpr(newPredTree->getValueId(), ex_expr::exp_SCAN_PRED, &postPred);
  }

  // Reset ATP's to zero for parent.
  //
  localMapTable->setAllAtp(0);

  // Generate expression to evaluate the cancel expression
  //
  ex_expr *cancelExpression = 0;

  if (!cancelExpr().isEmpty()) {
    ItemExpr *newCancelExprTree = cancelExpr().rebuildExprTree(ITM_AND, TRUE, TRUE);

    expGen->generateExpr(newCancelExprTree->getValueId(), ex_expr::exp_SCAN_PRED, &cancelExpression);
  }

  //
  //  For overflow
  //
  // ( The following are meaningless if ! unlimitedHistoryRows() )
  NABoolean noOverflow = CmpCommon::getDefault(EXE_BMO_DISABLE_OVERFLOW) == DF_ON;
  NABoolean logDiagnostics = CmpCommon::getDefault(EXE_DIAGNOSTIC_EVENTS) == DF_ON;
  NABoolean possibleMultipleCalls = generator->getRightSideOfFlow();
  short scratchTresholdPct = (short)CmpCommon::getDefaultLong(SCRATCH_FREESPACE_THRESHOLD_PERCENT);
  // determione the memory usage (amount of memory as percentage from total
  // physical memory used to initialize data structures)
  unsigned short memUsagePercent = (unsigned short)getDefault(BMO_MEMORY_USAGE_PERCENT);
  short memPressurePct = (short)getDefault(GEN_MEM_PRESSURE_THRESHOLD);

  historyRecLen = ROUND8(historyRecLen);

  int maxNumberOfOLAPBuffers;
  int maxRowsInOLAPBuffer;
  int minNumberOfOLAPBuffers;
  int numberOfWinOLAPBuffers;
  int olapBufferSize;

  computeHistoryParams(historyRecLen, maxRowsInOLAPBuffer, minNumberOfOLAPBuffers, numberOfWinOLAPBuffers,
                       maxNumberOfOLAPBuffers, olapBufferSize);

  ComTdbSequence *sequenceTdb = new (space)
      ComTdbSequence(readSeqExpr, returnExpr, postPred, cancelExpression, getMinFollowingRows(), historyRecLen,
                     historyAtpIndex, childTdb, givenCriDesc, returnCriDesc,
                     (queue_index)getDefault(GEN_SEQFUNC_SIZE_DOWN), (queue_index)getDefault(GEN_SEQFUNC_SIZE_UP),
                     getDefault(GEN_SEQFUNC_NUM_BUFFERS), getDefault(GEN_SEQFUNC_BUFFER_SIZE), olapBufferSize,
                     maxNumberOfOLAPBuffers, numHistoryRows(), getUnboundedFollowing(), logDiagnostics,
                     possibleMultipleCalls, scratchTresholdPct, memUsagePercent, memPressurePct, maxRowsInOLAPBuffer,
                     minNumberOfOLAPBuffers, numberOfWinOLAPBuffers, noOverflow, checkPartChangeExpr);
  sequenceTdb->setRecordLength(getGroupAttr()->getRecordLength());

  generator->initTdbFields(sequenceTdb);

  // update the estimated value of HistoryRowLength with actual value
  // setEstHistoryRowLength(historyIds.getRowLength());

  double sequenceMemEst = generator->getEstMemPerInst(getKey());
  generator->addToTotalEstimatedMemory(sequenceMemEst);

  if (!generator->explainDisabled()) {
    generator->setExplainTuple(addExplainInfo(sequenceTdb, childExplainTuple, 0, generator));
  }

  sequenceTdb->setScratchIOVectorSize((Int16)getDefault(SCRATCH_IO_VECTOR_SIZE_HASH));
  sequenceTdb->setOverflowMode(generator->getOverflowMode());

  sequenceTdb->setBmoMinMemBeforePressureCheck((Int16)getDefault(EXE_BMO_MIN_SIZE_BEFORE_PRESSURE_CHECK_IN_MB));

  if (generator->getOverflowMode() == ComTdb::OFM_SSD)
    sequenceTdb->setBMOMaxMemThresholdMB(
        (UInt16)(ActiveSchemaDB()->getDefaults()).getAsLong(SSD_BMO_MAX_MEM_THRESHOLD_IN_MB));
  else
    sequenceTdb->setBMOMaxMemThresholdMB(
        (UInt16)(ActiveSchemaDB()->getDefaults()).getAsLong(EXE_MEMORY_AVAILABLE_IN_MB));

  // The CQD EXE_MEM_LIMIT_PER_BMO_IN_MB has precedence over the mem quota sys
  NADefaults &defs = ActiveSchemaDB()->getDefaults();
  UInt16 mmu = (UInt16)(defs.getAsDouble(EXE_MEM_LIMIT_PER_BMO_IN_MB));
  UInt16 numBMOsInFrag = (UInt16)generator->getFragmentDir()->getNumBMOs();
  int numStreams;
  double bmoMemoryUsagePerNode = generator->getEstMemPerNode(getKey(), numStreams);
  double memQuota = 0;
  double memQuotaRatio;
  if (mmu != 0)
    sequenceTdb->setMemoryQuotaMB(mmu);
  else {
    // Apply quota system if either one the following two is true:
    //   1. the memory limit feature is turned off and more than one BMOs
    //   2. the memory limit feature is turned on
    NABoolean mlimitPerNode = defs.getAsDouble(BMO_MEMORY_LIMIT_PER_NODE_IN_MB) > 0;

    if (mlimitPerNode || numBMOsInFrag > 1) {
      double memQuota = computeMemoryQuota(generator->getEspLevel() == 0, mlimitPerNode,
                                           generator->getBMOsMemoryLimitPerNode().value(), generator->getTotalNumBMOs(),
                                           generator->getTotalBMOsMemoryPerNode().value(), numBMOsInFrag,
                                           bmoMemoryUsagePerNode, numStreams, memQuotaRatio);
    }
    int seqMemoryLowbound = defs.getAsLong(EXE_MEMORY_LIMIT_LOWER_BOUND_SEQUENCE);
    int memoryUpperbound = defs.getAsLong(BMO_MEMORY_LIMIT_UPPER_BOUND);

    if (memQuota < seqMemoryLowbound) {
      memQuota = seqMemoryLowbound;
      memQuotaRatio = BMOQuotaRatio::MIN_QUOTA;
    } else if (memQuota > memoryUpperbound)
      memQuota = memoryUpperbound;

    sequenceTdb->setMemoryQuotaMB(UInt16(memQuota));
  }

  generator->setCriDesc(givenCriDesc, Generator::DOWN);
  generator->setCriDesc(returnCriDesc, Generator::UP);
  generator->setGenObj(this, sequenceTdb);

  return 0;
}

void PhysSequence::seperateReadAndReturnItems(
    // ValueIdSet & readPhaseSet,
    // ValueIdSet & returnPhaseSet,
    CollHeap *wHeap) {
  ValueIdSet outputFromChild = child(0)->getGroupAttr()->getCharacteristicOutputs();

  ValueIdSet seqFuncs = sequenceFunctions();

  for (ValueId valId = seqFuncs.init(); seqFuncs.next(valId); seqFuncs.advance(valId)) {
    computeReadNReturnItems(valId, valId,
                            // sequenceFunctions(),
                            // returnSeqFunctions(),
                            outputFromChild, wHeap);
  }
}

void PhysSequence::transformOlapFunctions(CollHeap *wHeap) {
  for (ValueId valId = sequenceFunctions().init(); sequenceFunctions().next(valId);
       sequenceFunctions().advance(valId)) {
    ItemExpr *itmExpr = valId.getItemExpr();

    // NAType *itmType = itmExpr->getValueId().getType().newCopy(wHeap);

    if (itmExpr->isOlapFunction()) {
      NAType *itmType = itmExpr->getValueId().getType().newCopy(wHeap);

      itmExpr = ((ItmSeqOlapFunction *)itmExpr)->transformOlapFunction(wHeap);

      if (itmExpr->getOperatorType() == ITM_OLAP_LEAD) {
        computeAndSetMinFollowingRows(((ItmLeadOlapFunction *)itmExpr)->getOffset());
      }

      CMPASSERT(itmExpr);
      if (itmExpr->getValueId() != valId) {
        itmExpr = new (wHeap) Cast(itmExpr, itmType);
        itmExpr->synthTypeAndValueId(TRUE);
        valId.replaceItemExpr(itmExpr);
        itmExpr->getValueId().changeType(itmType);  //????
      }
    }
    itmExpr->transformOlapFunctions(wHeap);
  }
}

void PhysSequence::computeHistoryParams(int histRecLength, int &maxRowsInOLAPBuffer, int &minNumberOfOLAPBuffers,
                                        int &numberOfWinOLAPBuffers, int &maxNumberOfOLAPBuffers,
                                        int &olapBufferSize) {
  int maxFWAdditionalBuffers = getDefault(OLAP_MAX_FIXED_WINDOW_EXTRA_BUFFERS);
  maxNumberOfOLAPBuffers = getDefault(OLAP_MAX_NUMBER_OF_BUFFERS);
  // For testing we may force a smaller max # rows in a buffer
  int forceMaxRowsInOLAPBuffer = getDefault(OLAP_MAX_ROWS_IN_OLAP_BUFFER);
  minNumberOfOLAPBuffers = 0;
  numberOfWinOLAPBuffers = 0;
  olapBufferSize = getDefault(OLAP_BUFFER_SIZE);
  int olapAvailableBufferSize = olapBufferSize - ROUND8(sizeof(HashBufferHeader));  // header
  // also consider the trailer reserved for DP2's checksum, for overflow only
  if (getUnboundedFollowing()) olapAvailableBufferSize -= 8;

  if (histRecLength > olapAvailableBufferSize) {  // history row exceeds size limit fir the overflow
    if (getUnboundedFollowing()) {
      *CmpCommon::diags() << DgSqlCode(-4390) << DgInt0(olapAvailableBufferSize);
      GenExit();
      return;
    } else {
      olapAvailableBufferSize = histRecLength;
      // Buffer needs to accomodate the header, but not the trailer (no O/F)
      olapBufferSize = histRecLength + ROUND8(sizeof(HashBufferHeader));
    }
  }

  // Calculate the max # rows in a buffer
  maxRowsInOLAPBuffer = olapAvailableBufferSize / histRecLength;

  // For testing - we may override the above max # rows
  if (forceMaxRowsInOLAPBuffer > 0 && forceMaxRowsInOLAPBuffer < maxRowsInOLAPBuffer)
    maxRowsInOLAPBuffer = forceMaxRowsInOLAPBuffer;

  minNumberOfOLAPBuffers = numHistoryRows_ / maxRowsInOLAPBuffer;

  if (numHistoryRows_ % maxRowsInOLAPBuffer > 0) {
    minNumberOfOLAPBuffers++;
  }

  if (getUnboundedFollowing()) {
    numberOfWinOLAPBuffers = minFollowingRows_ / maxRowsInOLAPBuffer;
    if (minFollowingRows_ % maxRowsInOLAPBuffer > 0) {
      numberOfWinOLAPBuffers++;
    }

    if (numberOfWinOLAPBuffers + 1 > minNumberOfOLAPBuffers) {
      minNumberOfOLAPBuffers = numberOfWinOLAPBuffers + 1;
    }

    // produce an error here if maxNumberOfOLAPBuffers < minNumberOfOLAPBuffers
  } else {
    maxNumberOfOLAPBuffers = minNumberOfOLAPBuffers + maxFWAdditionalBuffers;
  }
}

CostScalar PhysSequence::getEstimatedRunTimeMemoryUsage(Generator *generator, NABoolean perNode, int *numStreams) {
  // input param is not used as this operator does not participate in the
  // quota system.

  ValueIdSet outputFromChild = child(0).getGroupAttr()->getCharacteristicOutputs();
  // TODO: Line below dumps core at times
  // const CostScalar maxCard = child(0).getGroupAttr()->getResultMaxCardinalityForEmptyInput();
  const CostScalar maxCard = 0;
  const CostScalar rowCount = numHistoryRows();

  // ValueIdSet historyIds;
  // getHistoryAttributes(sequenceFunctions(),outputFromChild, historyIds);
  // historyIds += sequenceFunctions();
  const int historyBufferWidthInBytes = getEstHistoryRowLength();  // historyIds.getRowLength();
  const double historyBufferSizeInBytes = rowCount.value() * historyBufferWidthInBytes;

  // totalMemory is per CPU at this point of time.
  double totalMemory = historyBufferSizeInBytes;
  CostScalar estMemPerNode;
  CostScalar estMemPerInst;

  const PhysicalProperty *const phyProp = getPhysicalProperty();
  int numOfStreams = 1;
  if (phyProp != NULL) {
    PartitioningFunction *partFunc = phyProp->getPartitioningFunction();
    numOfStreams = partFunc->getCountOfPartitions();
    if (numOfStreams <= 0) numOfStreams = 1;
    // totalMemory is for all CPUs at this point of time.
    totalMemory *= numOfStreams;
  }
  if (numStreams != NULL) *numStreams = numOfStreams;
  estMemPerNode = totalMemory /= MINOF(MAXOF(CURRCONTEXT_CLUSTERINFO->getTotalNumberOfCPUs(), 1), numOfStreams);
  estMemPerInst = totalMemory /= numOfStreams;
  OperBMOQuota *operBMOQuota =
      new (generator->wHeap()) OperBMOQuota(getKey(), numOfStreams, estMemPerNode, estMemPerInst, rowCount, maxCard);
  generator->getBMOQuotaMap()->insert(operBMOQuota);
  if (perNode)
    return estMemPerNode;
  else
    return estMemPerInst;
}

ExplainTuple *PhysSequence::addSpecificExplainInfo(ExplainTupleMaster *explainTuple, ComTdb *tdb,
                                                   Generator *generator) {
  NAString buffer = "num_history_rows: ";

  char buf[20];
  sprintf(buf, "%d ", numHistoryRows());
  buffer += buf;

  const int bufferWidth = ((ComTdbSequence *)tdb)->getRecLength();

  buffer += " history_row_size: ";
  sprintf(buf, "%d ", bufferWidth);
  buffer += buf;

  buffer += " est_history_row_size: ";
  sprintf(buf, "%d ", getEstHistoryRowLength());
  buffer += buf;

  // Int16 int16Val = ((ComTdbSequence *)tdb)->getUnboundedFollowing();
  Int16 int16Val = ((ComTdbSequence *)tdb)->isUnboundedFollowing();
  buffer += " unbounded_following: ";
  sprintf(buf, "%d ", int16Val);
  buffer += buf;

  int int32Val;
  int32Val = ((ComTdbSequence *)tdb)->getMaxRowsInOLAPBuffer();
  buffer += " max_rows_in_olap_buffer: ";
  sprintf(buf, "%d ", int32Val);
  buffer += buf;

  int32Val = ((ComTdbSequence *)tdb)->getMinNumberOfOLAPBuffers();
  buffer += " min_number_of_olap_buffer: ";
  sprintf(buf, "%d ", int32Val);
  buffer += buf;

  int32Val = ((ComTdbSequence *)tdb)->getMaxNumberOfOLAPBuffers();
  buffer += " max_number_of_olap_buffer: ";
  sprintf(buf, "%d ", int32Val);
  buffer += buf;

  int32Val = ((ComTdbSequence *)tdb)->getOLAPBufferSize();
  buffer += " olap_buffer_size: ";
  sprintf(buf, "%d ", int32Val);
  buffer += buf;

  int32Val = ((ComTdbSequence *)tdb)->getNumberOfWinOLAPBuffers();
  buffer += " number_of_win_olap_buffers: ";
  sprintf(buf, "%d ", int32Val);
  buffer += buf;

  int16Val = !((ComTdbSequence *)tdb)->isNoOverflow();
  buffer += " overflow_enabled: ";
  sprintf(buf, "%d ", int16Val);
  buffer += buf;

  int32Val = ((ComTdbSequence *)tdb)->getMinFollowing();
  buffer += " minimum_following: ";
  sprintf(buf, "%d ", int32Val);
  buffer += buf;

  explainTuple->setDescription(buffer);

  return (explainTuple);
}

void PhysSequence::computeReadNReturnItems(ValueId topSeqVid, ValueId vid, const ValueIdSet &outputFromChild,
                                           CollHeap *wHeap) {
  ItemExpr *itmExpr = vid.getItemExpr();

  if (outputFromChild.contains(vid)) {
    return;
  }

  // Make sure both the return and returning function
  // contain the same LEAD function. later on in
  // Sequence::codeGen() near seperateReadAndReturnItems(),
  // the function is separated as follows.
  //
  // LEAD in old form: LEAD(c)
  // LEAD in new form: LEAD(CONVERT(c))
  //
  // After separation:
  //
  //    readFunction: COVERT(C)
  //    returnFunction: LEAD(CONVERT(C))
  //
  //  In particular, both functions refer to CONVERT(C)
  //  with the same valueId.
  //
  if (itmExpr->getOperatorType() == ITM_OLAP_LEAD) {
    readSeqFunctions() += topSeqVid;
    returnSeqFunctions() += topSeqVid;
    return;
  }

  // test if itm_minus and then if negative offset ....
  if (itmExpr->getOperatorType() == ITM_OFFSET && ((ItmSeqOffset *)itmExpr)->getOffsetConstantValue() < 0) {
    readSeqFunctions() -= topSeqVid;
    returnSeqFunctions() += topSeqVid;

    readSeqFunctions() += itmExpr->child(0)->castToItemExpr()->getValueId();
    return;
  }

  if (itmExpr->getOperatorType() == ITM_MINUS) {
    ItemExpr *chld0 = itmExpr->child(0)->castToItemExpr();
    if (chld0->getOperatorType() == ITM_OFFSET && ((ItmSeqOffset *)chld0)->getOffsetConstantValue() < 0) {
      readSeqFunctions() -= topSeqVid;
      returnSeqFunctions() += topSeqVid;

      readSeqFunctions() += chld0->child(0)->castToItemExpr()->getValueId();

      ItemExpr *chld1 = itmExpr->child(1)->castToItemExpr();
      if (chld1->getOperatorType() == ITM_OFFSET && ((ItmSeqOffset *)chld1)->getOffsetConstantValue() < 0) {
        readSeqFunctions() += chld1->child(0)->castToItemExpr()->getValueId();
      } else {
        readSeqFunctions() += chld1->getValueId();
      }
      return;
    }
  }

  if (itmExpr->getOperatorType() == ITM_OLAP_MIN || itmExpr->getOperatorType() == ITM_OLAP_MAX) {
    ItmSeqOlapFunction *olap = (ItmSeqOlapFunction *)itmExpr;
    if (olap->getframeEnd() > 0) {
      readSeqFunctions() -= topSeqVid;
      returnSeqFunctions() += topSeqVid;

      ItemExpr *newChild = new (wHeap) Convert(itmExpr->child(0)->castToItemExpr());
      newChild->synthTypeAndValueId(TRUE);

      itmExpr->child(0) = newChild;

      readSeqFunctions() += newChild->getValueId();
      return;
    }
  }

  if (itmExpr->getOperatorType() == ITM_SCALAR_MIN || itmExpr->getOperatorType() == ITM_SCALAR_MAX) {
    ItemExpr *chld0 = itmExpr->child(0)->castToItemExpr();
    ItemExpr *chld1 = itmExpr->child(1)->castToItemExpr();
    if ((chld0->getOperatorType() == ITM_OLAP_MIN && chld1->getOperatorType() == ITM_OLAP_MIN) ||
        (chld0->getOperatorType() == ITM_OLAP_MAX && chld1->getOperatorType() == ITM_OLAP_MAX)) {
      ItmSeqOlapFunction *olap0 = (ItmSeqOlapFunction *)chld0;
      ItmSeqOlapFunction *olap1 = (ItmSeqOlapFunction *)chld1;
      if (olap1->getframeEnd() > 0) {
        CMPASSERT(olap0->getframeEnd() == 0);

        readSeqFunctions() -= topSeqVid;
        returnSeqFunctions() += topSeqVid;
        readSeqFunctions() += olap0->getValueId();

        ItemExpr *newChild = new (wHeap) Convert(olap1->child(0)->castToItemExpr());
        newChild->synthTypeAndValueId(TRUE);

        olap1->child(0) = newChild;

        readSeqFunctions() += newChild->getValueId();
      } else {
        CMPASSERT(olap1->getframeEnd() == 0);

        readSeqFunctions() -= topSeqVid;
        returnSeqFunctions() += topSeqVid;
        readSeqFunctions() += olap1->getValueId();

        ItemExpr *newChild = new (wHeap) Convert(olap0->child(0)->castToItemExpr());
        newChild->synthTypeAndValueId(TRUE);

        olap0->child(0) = newChild;

        readSeqFunctions() += newChild->getValueId();
      }
      return;
    }
  }

  for (int i = 0; i < itmExpr->getArity(); i++) {
    ItemExpr *chld = itmExpr->child(i);
    computeReadNReturnItems(topSeqVid, chld->getValueId(), outputFromChild, wHeap);
  }
}  // void PhysSequence::computeReadNReturnItems(ItemExpr * other)
