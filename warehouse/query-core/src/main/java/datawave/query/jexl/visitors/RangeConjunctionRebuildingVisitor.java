package datawave.query.jexl.visitors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import datawave.query.Constants;
import datawave.query.composite.CompositeUtils;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.IllegalRangeArgumentException;
import datawave.query.index.stats.IndexStatsClient;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.jexl.lookups.IndexLookupMap;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.planner.pushdown.Cost;
import datawave.query.planner.pushdown.CostEstimator;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.apache.commons.jexl2.parser.JexlNodes.id;

/**
 * Visits an JexlNode tree, removing bounded ranges (a pair consisting of one GT or GE and one LT or LE node), and replacing them with concrete equality nodes.
 * The concrete equality nodes will be replaced with normalized values because the TextNormalizer interface can only normalize a value and cannot un-normalize a
 * value.
 *
 * 
 *
 */
public class RangeConjunctionRebuildingVisitor extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(RangeConjunctionRebuildingVisitor.class);
    
    private final ShardQueryConfiguration config;
    private final ScannerFactory scannerFactory;
    private final IndexStatsClient stats;
    protected CostEstimator costAnalysis;
    protected Set<String> indexOnlyFields;
    protected Set<String> allFields;
    protected MetadataHelper helper;
    
    public RangeConjunctionRebuildingVisitor(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper)
                    throws TableNotFoundException, ExecutionException {
        this.config = config;
        this.helper = helper;
        this.indexOnlyFields = helper.getIndexOnlyFields(config.getDatatypeFilter());
        this.allFields = helper.getAllFields(config.getDatatypeFilter());
        this.scannerFactory = scannerFactory;
        stats = new IndexStatsClient(this.config.getConnector(), this.config.getIndexStatsTableName());
        costAnalysis = new CostEstimator(config, scannerFactory, helper);
    }
    
    /**
     * Expand all regular expression nodes into a conjunction of discrete terms mapping to that regular expression. For regular expressions that match nothing
     * in the global index, the regular expression node is left intact.
     *
     * @param config
     * @param helper
     * @param script
     * @return
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T expandRanges(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, T script)
                    throws TableNotFoundException, ExecutionException {
        RangeConjunctionRebuildingVisitor visitor = new RangeConjunctionRebuildingVisitor(config, scannerFactory, helper);
        
        if (null == visitor.config.getQueryFieldsDatatypes()) {
            QueryException qe = new QueryException(DatawaveErrorCode.DATATYPESFORINDEXFIELDS_MULTIMAP_MISSING);
            throw new DatawaveFatalQueryException(qe);
        }
        
        T node = (T) (script.jjtAccept(visitor, null));
        
        return node;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (ASTDelayedPredicate.instanceOf(node) || IndexHoleMarkerJexlNode.instanceOf(node)) {
            return node;
        } else if (ExceededValueThresholdMarkerJexlNode.instanceOf(node) || ExceededTermThresholdMarkerJexlNode.instanceOf(node)
                        || ExceededOrThresholdMarkerJexlNode.instanceOf(node)) {
            return node;
        } else
            return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return node;
    }
    
    // public Map<LiteralRange<?>,List<JexlNode>> expandCompositeRanges(Map<LiteralRange<?>,List<JexlNode>> ranges) throws Exception {
    // Map<LiteralRange<?>,List<JexlNode>> expandedRanges = new HashMap<>();
    // for (Map.Entry<LiteralRange<?>,List<JexlNode>> range : ranges.entrySet()) {
    // String fieldName = range.getKey().getFieldName();
    // LiteralRange<?> litRange = range.getKey();
    // String[] lowerBound = litRange.getLower() instanceof String ? ((String) litRange.getLower()).split(Constants.MAX_UNICODE_STRING) : null;
    // String[] upperBound = litRange.getUpper() instanceof String ? ((String) litRange.getUpper()).split(Constants.MAX_UNICODE_STRING) : null;
    //
    // if (config.getCompositeToFieldMap().keySet().contains(fieldName) && lowerBound != null && lowerBound.length > 0 && upperBound != null
    // && upperBound.length > 0) {
    // boolean lowerInclusive = litRange.isLowerInclusive();
    // boolean upperInclusive = litRange.isUpperInclusive();
    // LiteralRange.NodeOperand operand = litRange.getNodeOperand();
    //
    // List<Pair<String,String>> newRanges = new ArrayList<>();
    // Collection<String> componentFields = config.getCompositeToFieldMap().get(fieldName);
    // int fieldIdx = 0;
    // for (String compField : componentFields) {
    // String lowerComp = (lowerBound.length > fieldIdx) ? lowerBound[fieldIdx] : null;
    // String upperComp = (upperBound.length > fieldIdx) ? upperBound[fieldIdx] : null;
    //
    // if (helper.getFixedLengthCompositeFields().contains(compField) && lowerComp != null && upperComp != null) {
    // List<String> values = expandRange(lowerComp, upperComp);
    //
    // if (newRanges.size() > 0) {
    // newRanges = newRanges.stream().flatMap(x -> buildRanges(x, values).stream()).collect(Collectors.toList());
    // } else {
    // newRanges = values.stream().map(x -> new Pair<>(x, x)).collect(Collectors.toList());
    // }
    // fieldIdx++;
    // } else {
    // StringBuilder lowerBoundTail = new StringBuilder();
    // for (int i = fieldIdx; i < lowerBound.length; i++)
    // lowerBoundTail.append(Constants.MAX_UNICODE_STRING).append(lowerBound[i]);
    // StringBuilder upperBoundTail = new StringBuilder();
    // for (int i = fieldIdx; i < upperBound.length; i++)
    // upperBoundTail.append(Constants.MAX_UNICODE_STRING).append(upperBound[i]);
    //
    // newRanges = newRanges.stream()
    // .map(x -> new Pair<>(x.getValue0() + lowerBoundTail.toString(), x.getValue1() + upperBoundTail.toString()))
    // .collect(Collectors.toList());
    // break;
    // }
    // }
    // newRanges.forEach(x -> expandedRanges.put(new LiteralRange<String>(x.getValue0(), lowerInclusive, x.getValue1(), upperInclusive, fieldName,
    // operand), buildRangeNodes(x.getValue0(), lowerInclusive, x.getValue1(), upperInclusive, fieldName)));
    // } else {
    // expandedRanges.put(range.getKey(), range.getValue());
    // }
    // }
    // return expandedRanges;
    // }
    
    // public List<LiteralRange<?>> expandCompositeRange(LiteralRange<?> range) throws Exception {
    // List<Range> batchedRanges = new ArrayList<>();
    //
    // Key startKey = range.getStartKey();
    // Key endKey = range.getEndKey();
    //
    // String fieldName = startKey.getColumnFamily().toString();
    //
    // String[] lowerBound = startKey.getRow().toString().split(Constants.MAX_UNICODE_STRING);
    // String[] upperBound = endKey.getRow().toString().split(Constants.MAX_UNICODE_STRING);
    //
    // if (config.getCompositeToFieldMap().keySet().contains(fieldName)) {
    // List<Pair<String,String>> newRanges = new ArrayList<>();
    // Collection<String> componentFields = config.getCompositeToFieldMap().get(fieldName);
    // int fieldIdx = 0;
    // for (String compField : componentFields) {
    // String lowerComp = (lowerBound.length > fieldIdx) ? lowerBound[fieldIdx] : null;
    // String upperComp = (upperBound.length > fieldIdx) ? upperBound[fieldIdx] : null;
    //
    // if (helper.getFixedLengthCompositeFields().contains(compField) && lowerComp != null && upperComp != null) {
    // List<String> values = expandRange(lowerComp, upperComp);
    //
    // if (newRanges.size() > 0) {
    // newRanges = newRanges.stream().flatMap(x -> buildRanges(x, values).stream()).collect(Collectors.toList());
    // } else {
    // newRanges = values.stream().map(x -> new Pair<>(x, x)).collect(Collectors.toList());
    // }
    // fieldIdx++;
    // } else {
    // StringBuilder lowerBoundTail = new StringBuilder();
    // for (int i = fieldIdx; i < lowerBound.length; i++)
    // lowerBoundTail.append(Constants.MAX_UNICODE_STRING).append(lowerBound[i]);
    // StringBuilder upperBoundTail = new StringBuilder();
    // for (int i = fieldIdx; i < upperBound.length; i++)
    // upperBoundTail.append(Constants.MAX_UNICODE_STRING).append(upperBound[i]);
    //
    // newRanges = newRanges.stream()
    // .map(x -> new Pair<>(x.getValue0() + lowerBoundTail.toString(), x.getValue1() + upperBoundTail.toString()))
    // .collect(Collectors.toList());
    // break;
    // }
    // }
    // newRanges.forEach(x -> batchedRanges.add(
    // new Range(
    // new Key(new Text(x.getValue0()), startKey.getColumnFamily(), startKey.getColumnQualifier(), startKey.getColumnVisibility(), startKey.getTimestamp()),
    // new Key(new Text(x.getValue1()), endKey.getColumnFamily(), endKey.getColumnQualifier(), endKey.getColumnVisibility(), endKey.getTimestamp()))));
    // } else {
    // batchedRanges.add(range);
    // }
    // return batchedRanges;
    // }
    
    public List<LiteralRange<?>> expandCompositeRanges(LiteralRange<?> range) {
        List<LiteralRange<?>> batchedRanges = new ArrayList<>();
        // String fieldName = range.getFieldName();
        // String[] lowerBound = range.getLower() instanceof String ? ((String) range.getLower()).split(Constants.MAX_UNICODE_STRING) : null;
        // String[] upperBound = range.getUpper() instanceof String ? ((String) range.getUpper()).split(Constants.MAX_UNICODE_STRING) : null;
        
        // if (config.getCompositeToFieldMap().keySet().contains(fieldName) && lowerBound != null && lowerBound.length > 0 && upperBound != null
        // && upperBound.length > 0) {
        // boolean lowerInclusive = range.isLowerInclusive();
        // boolean upperInclusive = range.isUpperInclusive();
        // LiteralRange.NodeOperand operand = range.getNodeOperand();
        //
        // List<Pair<String,String>> newRanges = new ArrayList<>();
        // Collection<String> componentFields = config.getCompositeToFieldMap().get(fieldName);
        // int fieldIdx = 0;
        // for (String compField : componentFields) {
        // String lowerComp = (lowerBound.length > fieldIdx) ? lowerBound[fieldIdx] : null;
        // String upperComp = (upperBound.length > fieldIdx) ? upperBound[fieldIdx] : null;
        //
        // if (config.getFixedLengthFields().contains(compField) && lowerComp != null && upperComp != null) {
        // List<String> values = expandHexRange(lowerComp, upperComp);
        //
        // if (newRanges.size() > 0) {
        // newRanges = newRanges.stream().flatMap(x -> buildRanges(x, values).stream()).collect(Collectors.toList());
        // } else {
        // newRanges = values.stream().map(x -> new Pair<>(x, x)).collect(Collectors.toList());
        // }
        // fieldIdx++;
        // } else {
        // StringBuilder lowerBoundTail = new StringBuilder();
        // for (int i = fieldIdx; i < lowerBound.length; i++)
        // lowerBoundTail.append(Constants.MAX_UNICODE_STRING).append(lowerBound[i]);
        // StringBuilder upperBoundTail = new StringBuilder();
        // for (int i = fieldIdx; i < upperBound.length; i++)
        // upperBoundTail.append(Constants.MAX_UNICODE_STRING).append(upperBound[i]);
        //
        // newRanges = newRanges.stream().map(x -> new Pair<>(x.getValue0() + lowerBoundTail.toString(), x.getValue1() + upperBoundTail.toString()))
        // .collect(Collectors.toList());
        // break;
        // }
        // }
        // newRanges.forEach(x -> batchedRanges.add(new LiteralRange<String>(x.getValue0(), lowerInclusive, x.getValue1(), upperInclusive, fieldName,
        // operand)));
        // } else {
        batchedRanges.add(range);
        // }
        return batchedRanges;
    }
    
    // TODO: Change this back so it works for all types, not just hex
    // private List<String> expandRange(String lower, String upper) {
    // List<String> values = new ArrayList<>();
    // for (String value = lower; value.compareTo(upper) <= 0; value = CompositeUtils.incrementBound(value)) {
    // char lastChar = value.charAt(value.length() - 1);
    // if ((lastChar >= 'a' && lastChar <= 'f') || (lastChar >= '0' && lastChar <= '9'))
    // values.add(value);
    // }
    // return values;
    // }
    
    private List<String> expandHexRange(String lower, String upper) {
        int startInt = Integer.parseInt(lower, 16);
        int finishInt = Integer.parseInt(upper, 16);
        
        String format = "%0" + lower.length() + "x";
        
        List<String> result = new ArrayList<>();
        for (int i = startInt; i <= finishInt; i++)
            result.add(String.format(format, i));
        return result;
    }
    
    private List<Pair<String,String>> buildRanges(Pair<String,String> srcRange, List<String> values) {
        List<Pair<String,String>> newRanges = new ArrayList<>();
        for (String value : values)
            newRanges.add(new Pair<>(srcRange.getValue0() + Constants.MAX_UNICODE_STRING + value, srcRange.getValue1() + Constants.MAX_UNICODE_STRING + value));
        return newRanges;
    }
    
    // private List<JexlNode> buildRangeNodes(String lower, boolean lowerInclusive, String upper, boolean upperInclusive, String fieldName) {
    // List<JexlNode> nodes = new ArrayList<>();
    // nodes.add((lowerInclusive ? JexlNodeFactory.buildNode((ASTGENode) null, fieldName, lower) : JexlNodeFactory.buildNode((ASTGTNode) null, fieldName,
    // lower)));
    // nodes.add((upperInclusive ? JexlNodeFactory.buildNode((ASTLENode) null, fieldName, upper) : JexlNodeFactory.buildNode((ASTLTNode) null, fieldName,
    // upper)));
    // return nodes;
    // }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        List<JexlNode> leaves = new ArrayList<>();
        Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRanges(node, this.config.getDatatypeFilter(), this.helper, leaves, false);
        
        // THIS IS TOO EXPENSIVE. TRY USING ORIGINAL RANGES AND SEEKING INSTEAD
        // try {
        // ranges = expandCompositeRanges(ranges);
        // } catch (Exception e) {
        // System.out.println("WHITNEY: BUMMER!! Could not create sub ranges.");
        // }
        
        JexlNode andNode = JexlNodes.newInstanceOfType(node);
        andNode.image = node.image;
        andNode.jjtSetParent(node.jjtGetParent());
        
        // We have a bounded range completely inside of an AND/OR
        if (!ranges.isEmpty()) {
            andNode = expandIndexBoundedRange(ranges, leaves, node, andNode, data);
        } else {
            // We have no bounded range to replace, just proceed as normal
            JexlNodes.ensureCapacity(andNode, node.jjtGetNumChildren());
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode newChild = (JexlNode) node.jjtGetChild(i).jjtAccept(this, data);
                
                andNode.jjtAddChild(newChild, i);
                newChild.jjtSetParent(andNode);
            }
        }
        
        return andNode;
    }
    
    protected JexlNode expandIndexBoundedRange(Map<LiteralRange<?>,List<JexlNode>> ranges, List<JexlNode> leaves, ASTAndNode currentNode, JexlNode newNode,
                    Object data) {
        // Add all children in this AND/OR which are not a part of the range
        JexlNodes.ensureCapacity(newNode, leaves.size() + ranges.size());
        int index = 0;
        for (; index < leaves.size(); index++) {
            log.debug(leaves.get(index).image);
            // Add each child which is not a part of the bounded range, visiting them first
            JexlNode visitedChild = (JexlNode) leaves.get(index).jjtAccept(this, null);
            newNode.jjtAddChild(visitedChild, index);
            visitedChild.jjtSetParent(newNode);
        }
        
        // Sanity check to ensure that we found some nodes (redundant since we couldn't have made a bounded LiteralRange in the first
        // place if we had found not range nodes)
        if (ranges.isEmpty()) {
            log.debug("Cannot find range operator nodes that encompass this query. Not proceeding with range expansion for this node.");
            return currentNode;
        }
        
        // TODO: This is where we need to take a single composite range and BREAK IT UP into MULTIPLE SMALLER RANGES
        // TODO: Hopefully this will help us to skip more entries, and get a result faster
        for (Map.Entry<LiteralRange<?>,List<JexlNode>> range : ranges.entrySet()) {
            JexlNode compositePredicate = null;
            
            // if this is a composite field, find the composite predicate, which will be
            // used to filter out composite terms which fall outside of our range
            String fieldName = range.getKey().getFieldName();
            if (config.getCompositeToFieldMap().keySet().contains(fieldName)) {
                Set<JexlNode> delayedCompositePredicates = leaves.stream()
                                .map(leaf -> CompositePredicateVisitor.findCompositePredicates(leaf, config.getCompositeToFieldMap().get(fieldName)))
                                .flatMap(Collection::stream).collect(Collectors.toSet());
                if (delayedCompositePredicates != null && delayedCompositePredicates.size() == 1)
                    compositePredicate = delayedCompositePredicates.stream().findFirst().get();
            }
            
            IndexLookup lookup = ShardIndexQueryTableStaticMethods.expandRange(expandCompositeRanges(range.getKey()), compositePredicate);
            
            IndexLookupMap fieldsToTerms = null;
            
            try {
                fieldsToTerms = lookup.lookup(config, scannerFactory, config.getMaxIndexScanTimeMillis());
            } catch (IllegalRangeArgumentException e) {
                log.info("Cannot expand "
                                + range
                                + " because it creates an invalid Accumulo Range. This is likely due to bad user input or failed normalization. This range will be ignored.");
                return RebuildingVisitor.copy(currentNode);
            }
            
            // If we have any terms that we expanded, wrap them in parens and add them to the parent
            ASTAndNode onlyRangeNodes = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
            
            JexlNodes.ensureCapacity(onlyRangeNodes, range.getValue().size());
            for (int i = 0; i < range.getValue().size(); i++) {
                onlyRangeNodes.jjtAddChild(range.getValue().get(i), i);
            }
            
            JexlNode orNode = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, new ASTEQNode(
                            ParserTreeConstants.JJTEQNODE), onlyRangeNodes, fieldsToTerms);
            
            // Set the parent and child pointers accordingly
            orNode.jjtSetParent(newNode);
            newNode.jjtAddChild(orNode, index++);
            
        }
        
        // If we had no other nodes than this bounded range, we can strip out the original parent
        if (newNode.jjtGetNumChildren() == 1) {
            newNode.jjtGetChild(0).jjtSetParent(newNode.jjtGetParent());
            return newNode.jjtGetChild(0);
        }
        
        return newNode;
    }
    
    /**
     * We only want to expand a range if it is more selective than a node it is ANDed with.
     *
     * @param node
     * @param range
     * @return
     */
    public boolean shouldExpandRangeBasedOnSelectivity(JexlNode node, LiteralRange<?> range) {
        return shouldExpandRangeBasedOnSelectivity(node, range, IndexStatsClient.DEFAULT_VALUE);
    }
    
    /**
     * We only want to expand a range if it is more selective than a node it is ANDed with.
     *
     * @param node
     * @param range
     * @param rangeSelectivity
     * @return
     */
    protected boolean shouldExpandRangeBasedOnSelectivity(JexlNode node, LiteralRange<?> range, Double rangeSelectivity) {
        switch (id(node)) {
            case ParserTreeConstants.JJTGENODE:
            case ParserTreeConstants.JJTGTNODE:
            case ParserTreeConstants.JJTLENODE:
            case ParserTreeConstants.JJTLTNODE:
            case ParserTreeConstants.JJTREFERENCE:
            case ParserTreeConstants.JJTREFERENCEEXPRESSION:
                // recurse up the tree
                return shouldExpandRangeBasedOnSelectivity(node.jjtGetParent(), range, rangeSelectivity);
            case ParserTreeConstants.JJTANDNODE:
                boolean foundChildSelectivity = false;
                if (rangeSelectivity.equals(IndexStatsClient.DEFAULT_VALUE)) {
                    // only want to fetch the range selectivity once
                    rangeSelectivity = JexlASTHelper.getNodeSelectivity(Sets.newHashSet(range.getFieldName()), config, stats);
                    if (log.isDebugEnabled())
                        log.debug("Range selectivity:" + rangeSelectivity.toString());
                }
                for (JexlNode child : JexlASTHelper.getEQNodes(node)) {
                    // Try to get selectivity for each child
                    Double childSelectivity = JexlASTHelper.getNodeSelectivity(child, config, stats);
                    
                    if (childSelectivity.equals(IndexStatsClient.DEFAULT_VALUE)) {
                        continue;
                    } else {
                        foundChildSelectivity = true;
                    }
                    
                    if (log.isDebugEnabled() && foundChildSelectivity)
                        log.debug("Max Child selectivity: " + childSelectivity);
                    
                    // If the child is an EQ node, is indexed, and is more
                    // selective than the regex we don't need to process the regex
                    if (JexlASTHelper.getIdentifierOpLiteral(child) != null && JexlASTHelper.isIndexed(child, config) && rangeSelectivity < childSelectivity) {
                        return false;
                    }
                }
                
                return shouldExpandRangeBasedOnSelectivity(node.jjtGetParent(), range, rangeSelectivity);
            default:
                return true;
        }
    }
    
    /**
     * Walks up an AST and evaluates subtrees as needed. This method will fail fast if we determine we do not have to process a regex, otherwise the entire tree
     * will be evaluated.
     * 
     * This method recurses upwards, searching for an AND or OR node in the lineage. Once of those nodes is found, then the subtree rooted at that node is
     * evaluated. The visit map is used to cache already evaluated subtrees, so moving to a parent will not cause a subtree to be evaluated along with its
     * unevaluated siblings.
     * 
     * @param node
     *            - node to consider
     * 
     * @param visited
     *            - a visit list that contains the computed values for subtrees already visited, in case they are needed
     * 
     * @return true - if a regex has to be expanded false - if a regex doesn't have to be expanded
     */
    private boolean ascendTree(JexlNode node, Map<JexlNode,Boolean> visited) {
        if (node == null) {
            return true;
        } else {
            switch (id(node)) {
                case ParserTreeConstants.JJTORNODE:
                case ParserTreeConstants.JJTANDNODE: {
                    boolean expand = descendIntoSubtree(node, visited);
                    if (expand) {
                        return ascendTree(node.jjtGetParent(), visited);
                    } else {
                        return expand;
                    }
                }
                default:
                    return ascendTree(node.jjtGetParent(), visited);
            }
        }
    }
    
    /**
     * Evaluates a subtree to see if it can prevent the expansion of a regular expression.
     * 
     * This method recurses down under three conditions:
     * 
     * 1) An OR is encountered. In this case the result of recursing down the subtrees rooted at each child is OR'd together and returned. 2) An AND is
     * encountered. In this case the result of recursing down the subtrees rooted at each child is AND'd together and returned. 3) Any node that is not an EQ
     * node and has only 1 child. If there are multiple children, this method returns true, indicating that the subtree cannot defeat a regex expansion.
     * 
     * If an EQ node is encountered, we check if it can defeat an expansion by returning the value of a call to `doesNodeSupportRegexExpansion` on the node.
     * 
     * @param node
     * 
     * @return true - if a regex has to be expanded false - if a regex doesn't have to be expanded
     */
    private boolean descendIntoSubtree(JexlNode node, Map<JexlNode,Boolean> visited) {
        switch (id(node)) {
            case ParserTreeConstants.JJTORNODE: {
                return computeExpansionForSubtree(node, Join.OR, visited);
            }
            case ParserTreeConstants.JJTANDNODE: {
                return computeExpansionForSubtree(node, Join.AND, visited);
            }
            case ParserTreeConstants.JJTEQNODE: {
                boolean expand = doesNodeSupportRegexExpansion(node);
                visited.put(node, expand);
                return expand;
            }
            default: {
                JexlNode[] children = children(node);
                if (children.length == 1) {
                    boolean expand = descendIntoSubtree(children[0], visited);
                    visited.put(node, expand);
                    return expand;
                } else {
                    return true;
                }
            }
        }
    }
    
    /**
     * If we have a literal equality on an indexed field, then this can be used to defeat a wild card expansion.
     * 
     * @return `true` if we should expand a regular expression node given this subtree `false` if we should not expand a regular expression node given this
     *         subtree
     */
    private boolean doesNodeSupportRegexExpansion(JexlNode node) {
        return !(node instanceof ASTEQNode && JexlASTHelper.getIdentifierOpLiteral(node) != null && JexlASTHelper.isIndexed(node, config));
    }
    
    /**
     * Abstraction to indicate whether to use {@code `&=` or `|=`} when processing a node's subtrees.
     */
    enum Join {
        AND, OR
    }
    
    /**
     * The cases for OR and AND in `descendIntoSubtree` were almost equal, save for the initial value for expand and the operator used to join the results of
     * each child. I made this little macro doohickey to allow the differences between the two processes to be abstracted away.
     * 
     */
    private boolean computeExpansionForSubtree(JexlNode node, Join join, Map<JexlNode,Boolean> visited) {
        boolean expand = Join.AND.equals(join);
        for (JexlNode child : children(node)) {
            Boolean computedValue = visited.get(child);
            if (computedValue == null) {
                computedValue = descendIntoSubtree(child, visited);
                visited.put(child, computedValue);
            }
            switch (join) {
                case AND:
                    expand &= computedValue;
                    break;
                case OR:
                    expand |= computedValue;
            }
        }
        visited.put(node, expand);
        return expand;
    }
    
    public void collapseAndSubtrees(ASTAndNode node, List<JexlNode> subTrees) {
        for (JexlNode child : children(node)) {
            if (ParserTreeConstants.JJTANDNODE == id(child)) {
                collapseAndSubtrees((ASTAndNode) child, subTrees);
            } else {
                subTrees.add(child);
            }
        }
    }
    
    public boolean shouldProcessRegexByCostWithChildren(List<JexlNode> children, Cost regexCost) {
        Preconditions.checkArgument(!children.isEmpty(), "We found an empty list of children for an AND which should at least contain an ERnode");
        
        Cost c = new Cost();
        
        for (JexlNode child : children) {
            Cost childCost = costAnalysis.computeCostForSubtree(child);
            
            if (log.isDebugEnabled()) {
                log.debug("Computed cost of " + childCost + " for:");
                for (String logLine : PrintingVisitor.formattedQueryStringList(child)) {
                    log.debug(logLine);
                }
            }
            
            // Use this child's cost if we have no current cost or it's less than the current cost
            if (0 != childCost.getOtherCost()) {
                if (0 != c.getOtherCost()) {
                    if (childCost.getOtherCost() < c.getOtherCost()) {
                        c = childCost;
                    }
                } else {
                    c = childCost;
                }
            }
        }
        
        return (regexCost.getERCost() + regexCost.getOtherCost()) < (c.getERCost() + c.getOtherCost());
    }
}
