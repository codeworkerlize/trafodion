
#include "generator/GenExpGenerator.h"
#include "optimizer/RelPackedRows.h"
#include "comexe/ComTdbSample.h"
#include "comexe/ExplainTupleMaster.h"
#include "exp/ExpCriDesc.h"
#include "generator/Generator.h"
#include "optimizer/AllRelExpr.h"
#include "optimizer/GroupAttr.h"

// PhysSample::preCodeGen() -------------------------------------------
// Perform local query rewrites such as for the creation and
// population of intermediate tables, for accessing partitioned
// data. Rewrite the value expressions after minimizing the dataflow
// using the transitive closure of equality predicates.
//
// PhysSample::preCodeGen() - is basically the same as the RelExpr::
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
RelExpr *PhysSample::preCodeGen(Generator *generator, const ValueIdSet &externalInputs, ValueIdSet &pulledNewInputs) {
  // Do nothing if this node has already been processed.
  //
  if (nodeIsPreCodeGenned()) return this;

  // Resolve the VEGReferences and VEGPredicates, if any, that appear
  // in the Characteristic Inputs, in terms of the externalInputs.
  //
  getGroupAttr()->resolveCharacteristicInputs(externalInputs);

  // My Characteristics Inputs become the external inputs for my children.
  // preCodeGen my only child.
  //
  ValueIdSet childPulledInputs;
  child(0) = child(0)->preCodeGen(generator, externalInputs, pulledNewInputs);
  if (!child(0).getPtr()) return NULL;

  // Process additional any additional inputs the child wants.
  //
  getGroupAttr()->addCharacteristicInputs(childPulledInputs);
  pulledNewInputs += childPulledInputs;

  // The sampledCols() only have access to the Input Values.
  // These can come from the parent or be the outputs of the child.
  // Compute the set of available values for the sampledCols() and use
  // these to resolve any VEG references that the sampledCols() may need.
  //
  ValueIdSet availableValues;
  getInputValuesFromParentAndChildren(availableValues);

  sampledColumns().replaceVEGExpressions(availableValues, getGroupAttr()->getCharacteristicInputs());

  // Ditto, for the balance expression.
  //
  balanceExpr().replaceVEGExpressions(availableValues, getGroupAttr()->getCharacteristicInputs());

  requiredOrder().replaceVEGExpressions(availableValues, getGroupAttr()->getCharacteristicInputs());

  // The selectionPred has access to only the output values generated by
  // Sequence and input values from the parent. Compute the set of available
  // values for the selectionPred and resolve any VEG references
  // that the selection predicates may need.
  //
  getInputAndPotentialOutputValues(availableValues);

  NABoolean replicatePredicates = TRUE;
  selectionPred().replaceVEGExpressions(availableValues, getGroupAttr()->getCharacteristicInputs(),
                                        FALSE,  // no key predicates here
                                        0 /* no need for idempotence here */, replicatePredicates);

  // Resolve VEG references in the outputs and remove redundant
  // outputs.
  //
  getGroupAttr()->resolveCharacteristicOutputs(availableValues, getGroupAttr()->getCharacteristicInputs());

  // Mark this node as done and return.
  //
  markAsPreCodeGenned();

  return this;
}  // PhysSample::preCodeGen

short PhysSample::codeGen(Generator *generator) {
  // Get a local handle on some of the generator objects.
  //
  CollHeap *wHeap = generator->wHeap();
  Space *space = generator->getSpace();
  MapTable *mapTable = generator->getMapTable();
  ExpGenerator *expGen = generator->getExpGenerator();

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

  // Geneate the sampling expression.
  //
  ex_expr *balExpr = NULL;
  int returnFactorOffset = 0;
  ValueId val;
  val = balanceExpr().init();
  if (balanceExpr().next(val)) expGen->generateSamplingExpr(val, &balExpr, returnFactorOffset);

  // Alias the sampleColumns() so that they reference the underlying
  // expressions directly. This is done to avoid having to generate and
  // execute a project expression that simply moves the columns from
  // one tupp to another to reflect the application of the sampledCol
  // function.
  //
  //   ValueId valId;
  //   for(valId = sampledColumns().init();
  //       sampledColumns().next(valId);
  //       sampledColumns().advance(valId))
  //     {
  //       MapInfo *mapInfoChild = localMapTable->getMapInfoAsIs
  // 	(valId.getItemExpr()->child(0)->castToItemExpr()->getValueId());
  //       GenAssert(mapInfoChild, "Sample::codeGen -- no child map info.");
  //       Attributes *attr = mapInfoChild->getAttr();
  //       MapInfo *mapInfo = localMapTable->addMapInfoToThis(valId, attr);
  //       mapInfo->codeGenerated();
  //     }
  // columns. If so, return an error.
  ValueId valId;
  for (valId = sampledColumns().init(); sampledColumns().next(valId); sampledColumns().advance(valId)) {
    const NAType &colType = valId.getType();

    if (colType.isComposite()) {
      *CmpCommon::diags() << DgSqlCode(-4324);
      GenExit();
    }
  }
  // Now, remove all attributes from the map table except the
  // the stuff in the local map table -- the result of this node.
  //
  //  localMapTable->removeAll();

  // Generate the expression to evaluate predicate on the sampled row.
  //
  ex_expr *postPred = 0;
  if (!selectionPred().isEmpty()) {
    ItemExpr *newPredTree = selectionPred().rebuildExprTree(ITM_AND, TRUE, TRUE);

    expGen->generateExpr(newPredTree->getValueId(), ex_expr::exp_SCAN_PRED, &postPred);
  }

  // Construct the Sample TDB.
  //
  ComTdbSample *sampleTdb = new (space)
      ComTdbSample(NULL, balExpr, returnFactorOffset, postPred, childTdb, givenCriDesc, childCriDesc,
                   (queue_index)getDefault(GEN_SAMPLE_SIZE_DOWN), (queue_index)getDefault(GEN_SAMPLE_SIZE_UP));

  sampleTdb->setRecordLength(getGroupAttr()->getRecordLength());

  generator->initTdbFields(sampleTdb);

  if (!generator->explainDisabled()) {
    generator->setExplainTuple(addExplainInfo(sampleTdb, childExplainTuple, 0, generator));
  }

  generator->setCriDesc(givenCriDesc, Generator::DOWN);
  generator->setCriDesc(childCriDesc, Generator::UP);
  generator->setGenObj(this, sampleTdb);

  return 0;
}

ExplainTuple *PhysSample::addSpecificExplainInfo(ExplainTupleMaster *explainTuple, ComTdb *tdb, Generator *generator) {
  NAString buffer = "sample_type: ";

  switch (sampleType()) {
    case RANDOM:
      buffer += "RANDOM ";
      break;
    case PERIODIC:
      buffer += "PERIODIC ";
      break;
    case FIRSTN:
      buffer += "FIRST ";
      break;
    case CLUSTER:
      buffer += "CLUSTER ";
      break;
    default:
      buffer += "UNKNOWN ";
      break;
  }

  explainTuple->setDescription(buffer);

  return (explainTuple);
}
