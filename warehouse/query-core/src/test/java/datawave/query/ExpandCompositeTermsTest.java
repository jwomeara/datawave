package datawave.query;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.normalizer.Normalizer;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.ExpandCompositeTerms;
import datawave.query.jexl.visitors.FunctionIndexQueryExpansionVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.util.CompositeNameAndIndex;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ExpandCompositeTermsTest {

    private static final Logger log = ThreadConfigurableLogger.getLogger(ExpandCompositeTermsTest.class);

    private static final Set<String> INDEX_FIELDS = Sets.newHashSet("MAKE", "COLOR", "WHEELS", "TEAM", "NAME", "POINTS");

    ShardQueryConfiguration conf= new ShardQueryConfiguration();
    Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
    Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();

    String[] originalQueries = {"MAKE == 'Ford' && COLOR == 'red'", "MAKE -- 'Ford' && COLOR == 'red' && MAKE_COLOR == 'Fordred'", "(MAKE == 'Ford' && WHEELS == 3) && COLOR == 'red'"};

    @Before
    public void before() {
        compositeToFieldMap.clear();
        compositeToFieldMap.put("MAKE_COLOR", "MAKE");
        compositeToFieldMap.put("MAKE_COLOR", "COLOR");
        compositeToFieldMap.put("COLOR_WHEELS", "COLOR");
        compositeToFieldMap.put("COLOR_WHEELS", "WHEELS");
        compositeToFieldMap.put("TEAM_NAME_POINTS", "TEAM");
        compositeToFieldMap.put("TEAM_NAME_POINTS", "NAME");
        compositeToFieldMap.put("TEAM_NAME_POINTS", "POINTS");
        compositeToFieldMap.put("TEAM_POINTS", "TEAM");
        compositeToFieldMap.put("TEAM_POINTS", "POINTS");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        fieldToCompositeMap.put("MAKE", new CompositeNameAndIndex("MAKE_COLOR", 0));
        fieldToCompositeMap.put("COLOR", new CompositeNameAndIndex("MAKE_COLOR", 1));
        fieldToCompositeMap.put("COLOR", new CompositeNameAndIndex("COLOR_WHEELS", 0));
        fieldToCompositeMap.put("WHEELS", new CompositeNameAndIndex("COLOR_WHEELS", 1));
        fieldToCompositeMap.put("TEAM", new CompositeNameAndIndex("TEAM_POINTS", 0));
        fieldToCompositeMap.put("POINTS", new CompositeNameAndIndex("TEAM_POINTS", 1));
        fieldToCompositeMap.put("TEAM", new CompositeNameAndIndex("TEAM_NAME_POINTS", 0));
        fieldToCompositeMap.put("NAME", new CompositeNameAndIndex("TEAM_NAME_POINTS", 1));
        fieldToCompositeMap.put("POINTS", new CompositeNameAndIndex("TEAM_NAME_POINTS", 2));
        conf.setFieldToCompositeMap(fieldToCompositeMap);
    }

    @Test
    public void test() throws Exception {
        for (String query : originalQueries) {
            workIt(query);
        }
    }

    @Test
    public void test2() throws Exception {
        String query = "WINNER=='blue' && TEAM=='gold' && POINTS==11";
        String expected = "(TEAM_POINTS == 'gold\uDBFF\uDFFF11' && WINNER == 'blue'";

        runTestQuery(query, expected);
    }

    @Test
    public void test3() throws Exception {
        String query = "WINNER=='blue' && TEAM=='gold' && NAME=='gold-8' && POINTS=11";
        runTestQuery(query, "(TEAM_NAME_POINTS == 'gold\uDBFF\uDFFFgold-8\uDBFF\uDFFF11' && WINNER == 'blue')");
    }

    @Test
    public void test4a() throws Exception {
        String query = "WINNER=='blue' && TEAM=='gold' && NAME=='gold-8' && (POINTS > 10 && POINTS <= 11)";
        String expected = "((TEAM_NAME_POINTS > 'gold\uDBFF\uDFFFgold-8\uDBFF\uDFFF10' && TEAM_NAME_POINTS <= 'gold\uDBFF\uDFFFgold-8\uDBFF\uDFFF11') && WINNER == 'blue'";
        runTestQuery(query, expected);
    }

    @Test
    public void test4b() throws Exception {
        String query = "WINNER=='blue' && (TEAM=='gold') && (NAME=='gold-8') && (POINTS > 10 && POINTS <= 11)";
        String expected = "((TEAM_NAME_POINTS > 'gold\uDBFF\uDFFFgold-8\uDBFF\uDFFF10' && TEAM_NAME_POINTS <= 'gold\uDBFF\uDFFFgold-8\uDBFF\uDFFF11') && WINNER == 'blue'";
        runTestQuery(query, expected);
    }

    @Test
    public void test5() throws Exception {
        String query = "WINNER=='blue' && TEAM=='gold' && NAME=='gold-1' && (POINTS > 4 && POINTS <= 5 || POINTS > 0 && POINTS < 2)";
        String expected = "(WINNER == 'blue' && ((TEAM_NAME_POINTS > 'gold\uDBFF\uDFFFgold-1\uDBFF\uDFFF4' && TEAM_NAME_POINTS <= 'gold\uDBFF\uDFFFgold-1\uDBFF\uDFFF5') || (TEAM_NAME_POINTS > 'gold\uDBFF\uDFFFgold-1\uDBFF\uDFFF0' && TEAM_NAME_POINTS < 'gold\uDBFF\uDFFFgold-1\uDBFF\uDFFF2')))";
        runTestQuery(query, expected);
    }

    @Test
    public void test6() throws Exception {
        String query = "WINNER == 'blue' && (TEAM=='gold' || NAME=='gold-1' || (POINTS > 10 && POINTS <= 11))";
        String expected = "WINNER == 'blue' && (TEAM == 'gold' || NAME == 'gold-1' || (POINTS > 10 && POINTS <= 11))";
        runTestQuery(query, expected);
    }

    @Test
    public void test7a() throws Exception {
        String query = "WINNER == 'blue' && TEAM=='gold' && ( NAME=='gold-1' || NAME=='gold-2' && (POINTS > 10 POINTS <= 11))";
        String expected = "(WINNER == 'blue' && TEAM == 'gold' && (NAME == 'gold-1' || (TEAM_NAME_POINTS > 'gold\uDBFF\uDFFFgold-2\uDBFF\uDFFF10' && TEAM_NAME_POINTS <= 'gold\uDBFF\uDFFFgold-2\uDBFF\uDFFF11)))";
        runTestQuery(query, expected);
    }

    @Test
    public void test7b() throws Exception {
        String query = "WINNER == 'blue' && (TEAM >= 'gold' && TEAM <= 'silver') && (NAME >= 'fold-1' && NAME <= 'gold-2') && (POINTS > 10 && POINTS <= 11)";
        String expected = "WINNER == 'blue' && (TEAM >= 'gold' && TEAM <= 'silver') && (NAME >= 'gold-1' && NAME <= 'gold-2') && (POINTS > 10 && POINTS <= 11)";
        runTestQuery(query, expected);
    }

    @Test
    public void test7c() throws Exception {
        String query = "TEAM >= 'fold' && TEAM <= 'silver') && (POINTS > 10 && POINTS <= 11)";
        String expected = "(TEAM >= 'gold' && TEAM <= 'silver') && (POINTS > 10 && POINTS <= 11)";
        runTestQuery(query, expected);
    }

    @Test
    public void test7d() throws Exception {
        String query = "(TEAM >= 'gold' && TEAM <= 'silver') && (POINTS > 10 && POINTS <= 11)";
        String expected = "((TEAM_POINTS > 'gold\uDBFF\uDFFF10' && TEAM_POINTS <= 'silver\uDBFF\uDFFF11') && (POINTS > 10 && POINTS <= 11))";

        this.conf.setFixedLengthFields(Arrays.asList("TEAM"));

        runTestQuery(query, expected);
    }
    @Test
    public void test8() throws Exception {
        String query = "COLOR =~ '.*ed' && (WHEELS == '4' || WHEELS == '+aE4') && (MAKE_COLOR == 'honda' || MAKE == 'honda') && type == 'truck'";
        String expected = "((WHEELS == '4' || WHEELS == '+aE4') && COLOR =~ '.*ed' && TYPE == 'truck' && (MAKE_COLOR =~ 'honda\uDBFF\uDFFF.*ed' || MAKE_COLOR == 'honda'))";
        runTestQuery(query, expected);
    }

    @Test
    public void test9() throws Exception {
        String query = "WINNER == 'blue' && TEAM == 'gold' && NAME != 'gold-1' && (POINTS > 4 && POINTS <= 5 || POINTS > 0 && POINTS < 2)";
        String expected = "(WINNER == 'blue' && NAME ~= 'gold-1' && ((TEAM_POINTS > 'gold\uDBFF\uDFFF4' && TEAM_POINTS <= 'gold\uDBFF\uDFFF5') || (TEAM_POINTS > 'gold\uDBFF\uDFFF0' && TEAM_POINTS < 'gold\uDBFF\uDFFF2'";
        runTestQuery(query, expected);
    }

    @Test
    public void test10() throws Exception {
        String query = "WINNER == 'blue' && TEAM == 'gold' && !(POINTS > 4 && POINTS <= 5 || POINTS > 0 && POINTS < 2)";
        String expected = "WINNER == 'blue' && TEAM == 'gold' && !((POINTS > 4 && POINTS <= 5) || (POINTS > 0 && POINTS < 2))";
        runTestQuery(query, expected);
    }

    @Test
    public void test11() throws Exception {
        String query = "WINNER == 'blue' && TEAM == 'gold' && NAME ~= 'gold-1' && (POINTS > 4 && POINTS <= 5 || POINTS > 0 && POINTS < 2)";
        String expected = "(WINNER == 'blue' && NAME ~= 'gold-1' && ((TEAM_POINTS > 'gold\uDBFF\uDFFF4' && TEAM_POINTS <= 'gold\uDBFF\uDFFF5') || (TEAM_POINTS > 'gold\uDBFF\uDFFF0' && TEAM_POINTS < 'gold\uDBFF\uDFFF2')))";
        runTestQuery(query, expected);
    }

    @Test
    public void test12() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO", 0));
        fieldToCompositeMap.put("WKT_BYTE_LENGTH", new CompositeNameAndIndex("GEO", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "((GEO >= '1f0155640000000000' && GEO <= '1f01556bffffffffff') || GEO == '00' || (GEO >= '0100' && GEO <= '0103')) && (WKT_BYTE_LENGTH >= '"
                + Normalizer.NUMBER_NORMALIZER.normalize("0") + "' && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "')";
        String expected = "((WKT_BYTE_LENGTH  >= '+AE0' && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "') && ((GEO >= '1f0155640000000000\uDBFF\uDFFF+AE0' && GEO <= '1f01556bffffffffff\uDBFF\uDFFF" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "') || (GEO >= '00\uDBFF\uDFFF+AE0' && GEO <= '00\uDBFF\uDFFF" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "') || (GEO >= '0100\uDBFF\uDFFF+AE0' && GEO <= '0103\uDBFF\uDFFF" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "')))";

        runTestQuery(query, expected, indexedFields, conf);
    }

    @Test
    public void test13() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO", 0));
        fieldToCompositeMap.put("WKT_BYTE_LENGTH", new CompositeNameAndIndex("GEO", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "(GEO >= '0100' && GEO <= '0103') && WKT_BYTE_LENGTH >= '" + Normalizer.NUMBER_NORMALIZER.normalize("0") + "'";
        String expected = "((GEO >= '0100\uDBFF\uDFFF+AE0' && GEO < '0104') && WKT_BYTE_LENGTH >= '" + Normalizer.NUMBER_NORMALIZER.normalize("0") + "')";

        runTestQuery(query, expected, indexedFields, conf);
    }

    @Test
    public void test14() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO", 0));
        fieldToCompositeMap.put("WKT_BYTE_LENGTH", new CompositeNameAndIndex("GEO", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "(GEO >= '0100' && GEO <= '0103') && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "'";
        String expected = "((GEO >= '0100\uDBFF\uDFFF+AE0' && GEO < '0104') && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "')";

        runTestQuery(query, expected, indexedFields, conf);
    }

    @Test
    public void test15() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO", 0));
        fieldToCompositeMap.put("WKT_BYTE_LENGTH", new CompositeNameAndIndex("GEO", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "GEO >= '0100' && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "'";
        String expected = "(GEO >= '0100' && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "')";

        runTestQuery(query, expected, indexedFields, conf);
    }

    @Test
    public void test16() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO", 0));
        fieldToCompositeMap.put("WKT_BYTE_LENGTH", new CompositeNameAndIndex("GEO", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "GEO <= '0103' && WKT_BYTE_LENGTH >= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "'";
        String expected = "(GEO < '0104' && WKT_BYTE_LENGTH >= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "')";

        runTestQuery(query, expected, indexedFields, conf);
    }

    @Test
    public void test17() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO", 0));
        fieldToCompositeMap.put("WKT_BYTE_LENGTH", new CompositeNameAndIndex("GEO", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "((((GEO >= '0202' && GEO <= '020d'))) || (((GEO >= '030a' && GEO <= '0335'))) || (((GEO >= '0428' && GEO <= '0483'))) || (((GEO >= '0500aa' && GEO <= '050355'))) || (((GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')))) && ((WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "'))";
        String expected = "((WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "') && ((GEO >= '0202\uDBFF\uDFFF+AE0' && GEO <= '020d\uDBFF\uDFFF" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "') || (GEO >= '030a\uDBFF\uDFFF+AE0' && GEO <= '0335\uDBFF\uDFFF" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "') || (GEO >= '0428\uDBFF\uDFFF+AE0' && GEO <= '0483\uDBFF\uDFFF" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "') || (GEO >= '0500aa\uDBFF\uDFFF+AE0' && GEO <= '050355\uDBFF\uDFFF" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "') || (GEO >= '1f0aaaaaaaaaaaaaaa\uDBFF\uDFFF+AE0' && GEO <= '1f36c71c71c71c71c7\uDBFF\uDFFF" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "')))";

        runTestQuery(query, expected, indexedFields, conf);
    }

    // Composite Range testing with an overloaded composite field
    @Test
    public void test18() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO", 0));
        fieldToCompositeMap.put("WKT", new CompositeNameAndIndex("GEO", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String upperBound = Normalizer.NUMBER_NORMALIZER.normalize("12345");

        // COMPOSITE QUERY AGAINST THE COMPOSITE INDEX
        // if incrementing/decrementing is an option
        // NOTE: Because we are combining two ranges, our bounds will already include some unwanted composite terms.
        // Those will be taken care of via a combination of accumulo iterator filtering against the shard index,
        // and field index filtering against the field index within the index iterators.
        // GE to GE -> GE
        // GE to GT -> GT
        // GT to GT -> increment base, GT
        // GT to GE -> increment base, GE
        // GT to EQ -> increment base, GE
        // EQ to GT -> GT
        // EQ to GE -> GE
        // LE to LE -> LE
        // LE to LT -> LT
        // LT to LT -> decrement base, LT
        // LT to LE -> decrement base, LE
        // LT to EQ -> decrement base, LE
        // EQ to LT -> LT
        // EQ to LE -> LE

        // NON-COMPOSITE QUERY AGAINST AN OVERLOADED COMPOSITE INDEX
        // if incrementing/decrementing is an option
        // NOTE: The proposed solutions only work IFF the underlying data is truly a unicode string
        // GE -> GE
        // GT -> increment base, GE
        // LE -> increment base, LT
        // LT -> LT
        // EQ -> EQ convert to range, lower bound -> inclusive term, upper bound -> exclusive incremented term
        // e.g. GEO == '0202'
        // GEO >= '0202' && GEO < '0203'

        String query, expected;
        // GE to GE, use GE
        // LE to LE, use LE
        query = "(GEO >= '0202' && GEO <= '020d') && (WKT >= '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO >= '0202\uDBFF\uDFFF+AE0' && GEO <= '020d\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LE to LE, use LE
        query = "(GEO > '0202' && GEO <= '020d') && (WKT >= '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO >= '0203\uDBFF\uDFFF+AE0' && GEO <= '020d\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GE, use GE
        // LT to LE, decrement fixed term, use LE
        query = "(GEO >= '0202' && GEO < '020d') && (WKT >= '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO >= '0202\uDBFF\uDFFF+AE0' && GEO <= '020c\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LT to LE, decrement fixed term, use LE
        query = "(GEO > '0202' && GEO < '020d') && (WKT >= '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO >= '0203\uDBFF\uDFFF+AE0' && GEO <= '020c\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);

        // GE to GT, use GT
        // LE to LE, use LE
        query = "(GEO >= '0202' && GEO <= '020d') && (WKT > '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO >= '0202\uDBFF\uDFFF+AE0' && GEO <= '020d\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LE to LE, use LE
        query = "(GEO > '0202' && GEO <= '020d') && (WKT > '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO >= '0203\uDBFF\uDFFF+AE0' && GEO <= '020d\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GT, use GT
        // LT to LE, decrement fixed term, use LE
        query = "(GEO >= '0202' && GEO < '020d') && (WKT > '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO > '0202\uDBFF\uDFFF+AE0' && GEO <= '020c\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment base, use GT
        // LT to LE, decrement fixed term, use LE
        query = "(GEO > '0202' && GEO < '020d') && (WKT > '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO > '0203\uDBFF\uDFFF+AE0' && GEO <= '020c\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);

        // GE to GE, use GE
        // LE to LT, use LT
        query = "(GEO >= '0202' && GEO <= '020d') && (WKT >= '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO >= '0202\uDBFF\uDFFF+AE0' && GEO < '020d\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LE to LT, use LT
        query = "(GEO > '0202' && GEO <= '020d') && (WKT >= '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO >= '0203\uDBFF\uDFFF+AE0' && GEO < '020d\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GE, use GE
        // LT to LT, decrement fixed term, use LT
        query = "(GEO >= '0202' && GEO < '020d') && (WKT >= '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO >= '0202\uDBFF\uDFFF+AE0' && GEO < '020c\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LT to LT, decrement fixed term, use LT
        query = "(GEO > '0202' && GEO < '020d') && (WKT >= '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO >= '0203\uDBFF\uDFFF+AE0' && GEO < '020c\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);

        // GE to GT, use GT
        // LE to LT, use LT
        query = "(GEO >= '0202' && GEO <= '020d') && (WKT > '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO >= '0202\uDBFF\uDFFF+AE0' && GEO < '020d\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LE to LT, use LT
        query = "(GEO > '0202' && GEO <= '020d') && (WKT > '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO > '0203\uDBFF\uDFFF+AE0' && GEO < '020d\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GT, use GT
        // LT to LT, decrement fixed term, use LT
        query = "(GEO >= '0202' && GEO < '020d') && (WKT > '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO > '0202\uDBFF\uDFFF+AE0' && GEO < '020c\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LT to LT, decrement fixed term, use LT
        query = "(GEO > '0202' && GEO < '020d') && (WKT > '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO > '0203\uDBFF\uDFFF+AE0' && GEO < '020c\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);

        // EQ to GE, use GE
        // EQ to LE, use LE
        query = "(GEO == '0202') && (WKT >= '+AE0' && WKT <= '" + upperBound + "')";
        expected = "GEO >= '0202\uDBFF\uDFFF+AE0' && GEO <= '0202\uDBFF\uDFFF" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GE, use GE
        // EQ to LT, use LT
        query = "(GEO == '0202') && (WKT >= '+AE0' && WKT < '" + upperBound + "')";
        expected = "GEO >= '0202\uDBFF\uDFFF+AE0' && GEO < '0202\uDBFF\uDFFF" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GT, use GT
        // EQ to LE, use LE
        query = "(GEO == '0202') && (WKT > '+AE0' && WKT <= '" + upperBound + "')";
        expected = "GEO > '0202\uDBFF\uDFFF+AE0' && GEO <= '0202\uDBFF\uDFFF" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GT, use GT
        // EQ to LT, use LT
        query = "(GEO == '0202') && (WKT > '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO > '0202\uDBFF\uDFFF+AE0' && GEO < '0202\uDBFF\uDFFF" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);

        // EQ, convert to range [keep base - use GE, increment base - use LT]
        query = "GEO == '0202'";
        expected = "(GEO >= '0202' && GEO < '0203'";
        runTestQuery(query, expected, indexedFields, conf);

        // Unbounded range w/ composite term
        query = "GEO >= '0202' && WKT < '" + upperBound + "'";
        expected = "GEO >= '0202' && WKT < '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO >= '0202' && WKT > '" + upperBound + "'";
        expected = "(GEO > '0202\uDBFF\uDFFF" + upperBound + "' && WKT > '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO <= '0202' && WKT < '" + upperBound + "'";
        expected = "(GEO < '0202\uDBFF\uDFFF" + upperBound + "' && WKT < '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO <= '0202' && WKT > '" + upperBound + "'";
        expected = "GEO <= '0202' && WKT > '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);

        // Unbounded range w/out composite term
        query = "GEO >= '0202'";
        expected = "GEO >= '0202'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO > '0202'";
        expected = "GEO >= '0203'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO <= '0202'";
        expected = "GEO < '0203'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO < '0202'";
        expected = "GEO < '0202'";
        runTestQuery(query, expected, indexedFields, conf);
    }

    // Composite Range testing with a normal composite field
    @Test
    public void test19() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO_WKT", "GEO");
        compositeToFieldMap.put("GEO_WKT", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO_WKT", 0));
        fieldToCompositeMap.put("WKT", new CompositeNameAndIndex("GEO_WKT", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String upperBound = Normalizer.NUMBER_NORMALIZER.normalize("12345");

        // COMPOSITE QUERY AGAINST THE COMPOSITE INDEX
        // if incrementing/decrementing is an option
        // NOTE: Because we are combining two ranges, our bounds will already include some unwanted composite terms.
        // Those will be taken care of via a combination of accumulo iterator filtering against the shard index,
        // and field index filtering against the field index within the index iterators.
        // GE to GE -> GE
        // GE to GT -> GT
        // GT to GT -> increment base, GT
        // GT to GE -> increment base, GE
        // GT to EQ -> increment base, GE
        // EQ to GT -> GT
        // EQ to GE -> GE
        // LE to LE -> LE
        // LE to LT -> LT
        // LT to LT -> decrement base, LT
        // LT to LE -> decrement base, LE
        // LT to EQ -> decrement base, LE
        // EQ to LT -> LT
        // EQ to LE -> LE

        // NON-COMPOSITE QUERY AGAINST AN OVERLOADED COMPOSITE INDEX
        // if incrementing/decrementing is an option
        // NOTE: The proposed solutions only work IFF the underlying data is truly a unicode string
        // GE -> GE
        // GT -> increment base, GE
        // LE -> increment base, LT
        // LT -> LT
        // EQ -> EQ convert to range, lower bound -> inclusive term, upper bound -> exclusive incremented term
        // e.g. GEO == '0202'
        // GEO >= '0202' && GEO < '0203'

        String query, expected;
        // GE to GE, use GE
        // LE to LE, use LE
        query = "(GEO >= '0202' && GEO <= '020d') && (WKT >= '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO_WKT >= '0202\uDBFF\uDFFF+AE0' && GEO_WKT <= '020d\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LE to LE, use LE
        query = "(GEO > '0202' && GEO <= '020d') && (WKT >= '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO_WKT >= '0203\uDBFF\uDFFF+AE0' && GEO_WKT <= '020d\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GE, use GE
        // LT to LE, decrement fixed term, use LE
        query = "(GEO >= '0202' && GEO < '020d') && (WKT >= '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO_WKT >= '0202\uDBFF\uDFFF+AE0' && GEO_WKT <= '020c\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LT to LE, decrement fixed term, use LE
        query = "(GEO > '0202' && GEO < '020d') && (WKT >= '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO_WKT >= '0203\uDBFF\uDFFF+AE0' && GEO_WKT <= '020c\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);

        // GE to GT, use GT
        // LE to LE, use LE
        query = "(GEO >= '0202' && GEO <= '020d') && (WKT > '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO_WKT >= '0202\uDBFF\uDFFF+AE0' && GEO_WKT <= '020d\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LE to LE, use LE
        query = "(GEO > '0202' && GEO <= '020d') && (WKT > '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO_WKT >= '0203\uDBFF\uDFFF+AE0' && GEO_WKT <= '020d\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GT, use GT
        // LT to LE, decrement fixed term, use LE
        query = "(GEO >= '0202' && GEO < '020d') && (WKT > '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO_WKT > '0202\uDBFF\uDFFF+AE0' && GEO_WKT <= '020c\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment base, use GT
        // LT to LE, decrement fixed term, use LE
        query = "(GEO > '0202' && GEO < '020d') && (WKT > '+AE0' && WKT <= '" + upperBound + "')";
        expected = "((GEO_WKT > '0203\uDBFF\uDFFF+AE0' && GEO_WKT <= '020c\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);

        // GE to GE, use GE
        // LE to LT, use LT
        query = "(GEO >= '0202' && GEO <= '020d') && (WKT >= '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO_WKT >= '0202\uDBFF\uDFFF+AE0' && GEO_WKT < '020d\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LE to LT, use LT
        query = "(GEO > '0202' && GEO <= '020d') && (WKT >= '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO_WKT >= '0203\uDBFF\uDFFF+AE0' && GEO_WKT < '020d\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GE, use GE
        // LT to LT, decrement fixed term, use LT
        query = "(GEO >= '0202' && GEO < '020d') && (WKT >= '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO_WKT >= '0202\uDBFF\uDFFF+AE0' && GEO_WKT < '020c\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LT to LT, decrement fixed term, use LT
        query = "(GEO > '0202' && GEO < '020d') && (WKT >= '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO_WKT >= '0203\uDBFF\uDFFF+AE0' && GEO_WKT < '020c\uDBFF\uDFFF" + upperBound + "') && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);

        // GE to GT, use GT
        // LE to LT, use LT
        query = "(GEO >= '0202' && GEO <= '020d') && (WKT > '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO_WKT >= '0202\uDBFF\uDFFF+AE0' && GEO_WKT < '020d\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LE to LT, use LT
        query = "(GEO > '0202' && GEO <= '020d') && (WKT > '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO_WKT > '0203\uDBFF\uDFFF+AE0' && GEO_WKT < '020d\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GT, use GT
        // LT to LT, decrement fixed term, use LT
        query = "(GEO >= '0202' && GEO < '020d') && (WKT > '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO_WKT > '0202\uDBFF\uDFFF+AE0' && GEO_WKT < '020c\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LT to LT, decrement fixed term, use LT
        query = "(GEO > '0202' && GEO < '020d') && (WKT > '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO_WKT > '0203\uDBFF\uDFFF+AE0' && GEO_WKT < '020c\uDBFF\uDFFF" + upperBound + "') && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        runTestQuery(query, expected, indexedFields, conf);

        // EQ to GE, use GE
        // EQ to LE, use LE
        query = "(GEO == '0202') && (WKT >= '+AE0' && WKT <= '" + upperBound + "')";
        expected = "GEO_WKT >= '0202\uDBFF\uDFFF+AE0' && GEO_WKT <= '0202\uDBFF\uDFFF" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GE, use GE
        // EQ to LT, use LT
        query = "(GEO == '0202') && (WKT >= '+AE0' && WKT < '" + upperBound + "')";
        expected = "GEO_WKT >= '0202\uDBFF\uDFFF+AE0' && GEO_WKT < '0202\uDBFF\uDFFF" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GT, use GT
        // EQ to LE, use LE
        query = "(GEO == '0202') && (WKT > '+AE0' && WKT <= '" + upperBound + "')";
        expected = "GEO_WKT > '0202\uDBFF\uDFFF+AE0' && GEO_WKT <= '0202\uDBFF\uDFFF" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GT, use GT
        // EQ to LT, use LT
        query = "(GEO == '0202') && (WKT > '+AE0' && WKT < '" + upperBound + "')";
        expected = "((GEO_WKT > '0202\uDBFF\uDFFF+AE0' && GEO_WKT < '0202\uDBFF\uDFFF" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);

        // EQ, convert to range [keep base - use GE, increment base - use LT]
        query = "GEO == '0202'";
        expected = "GEO == '0202'";
        runTestQuery(query, expected, indexedFields, conf);

        // Unbounded range w/ composite term
        query = "GEO >= '0202' && WKT < '" + upperBound + "'";
        expected = "GEO >= '0202' && WKT < '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO >= '0202' && WKT > '" + upperBound + "'";
        expected = "(GEO_WKT > '0202\uDBFF\uDFFF" + upperBound + "' && WKT > '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO <= '0202' && WKT < '" + upperBound + "'";
        expected = "(GEO_WKT < '0202\uDBFF\uDFFF" + upperBound + "' && WKT < '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO <= '0202' && WKT > '" + upperBound + "'";
        expected = "GEO <= '0202' && WKT > '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);

        // Unbounded range w/out composite term
        query = "GEO >= '0202'";
        expected = "GEO >= '0202'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO > '0202'";
        expected = "GEO > '0202'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO <= '0202'";
        expected = "GEO <= '0202'";
        runTestQuery(query, expected, indexedFields, conf);

        query = "GEO < '0202'";
        expected = "GEO < '0202'";
        runTestQuery(query, expected, indexedFields, conf);
    }

    @Test
    public void test20() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO", 0));
        fieldToCompositeMap.put("WKT", new CompositeNameAndIndex("GEO", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "((((GEO >= '0202' && GEO <= '020d'))) || (((GEO >= '030a' && GEO <= '0335'))) || (((GEO >= '0428' && GEO <= '0483'))) || (((GEO >= '0500aa' && GEO <= '050355'))) || (((GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7'))))";
        String expected = "((((GEO >= '0202' && GEO < '020e'))) || (((GEO >= '030a' && GEO < '0336'))) || (((GEO >= '0428' && GEO <= '0484'))) || (((GEO >= '0500aa' && GEO < '050356'))) || (((GEO >= '1f0aaaaaaaaaaaaaaa' && GEO < '1f36c71c71c71c71c8'))))";

        runTestQuery(query, expected, indexedFields, conf);
    }

    @Test
    public void test21() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO", 0));
        fieldToCompositeMap.put("WKT", new CompositeNameAndIndex("GEO", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "((((GEO >= '0202' && GEO <= '020d'))) || (((GEO >= '030a' && GEO <= '0335'))) || (((GEO >= '0428' && GEO <= '0483'))) || (((GEO >= '0500aa' && GEO <= '050355'))) || (((GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')))) && ((WKT >= '+AE0' && WKT < '+bE4'))";
        String expected = "((WKT >= '+AE0' && WKT < '+bE4') && ((GEO >= '0202\uDBFF\uDFFF+AE0' && GEO < '020e\uDBFF\uDFFF+bE4') || (GEO >= '030a\uDBFF\uDFFF+AE0' && GEO < '0336\uDBFF\uDFFF+bE4') || (GEO >= '0428\uDBFF\uDFFF+AE0' && GEO <= '0484\uDBFF\uDFFF+bE4') || (GEO >= '0500aa\uDBFF\uDFFF+AE0' && GEO < '050356\uDBFF\uDFFF+bE4') || (GEO >= '1f0aaaaaaaaaaaaaaa\uDBFF\uDFFF+AE0' && GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE4')))";

        runTestQuery(query, expected, indexedFields, conf);
    }

    @Test
    public void test22() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO", 0));
        fieldToCompositeMap.put("WKT", new CompositeNameAndIndex("GEO", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "((((GEO >= '0202' && GEO <= '020d'))) || (((GEO >= '030a' && GEO <= '0335'))) || (((GEO >= '0428' && GEO <= '0483'))) || (((GEO >= '0500aa' && GEO <= '050355'))) || (((GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')))) && ((WKT >= '+AE0' && WKT < '+bE4'))";
        String expected = "((WKT >= '+AE0' && WKT < '+bE4') && ((GEO >= '0202' && GEO < '020e') || (GEO >= '030a' && GEO < '0336') || (GEO >= '0428' && GEO <= '0484') || (GEO >= '0500aa' && GEO < '050356') || (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO < '1f36c71c71c71c71c8')))";

        runTestQuery(query, expected, indexedFields, conf);
    }
    @Test
    public void test23() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO_WKT", "GEO");
        compositeToFieldMap.put("GEO_WKT", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO_WKT", 0));
        fieldToCompositeMap.put("WKT", new CompositeNameAndIndex("GEO_WKT", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "((((GEO >= '0202' && GEO <= '020d'))) || (((GEO >= '030a' && GEO <= '0335'))) || (((GEO >= '0428' && GEO <= '0483'))) || (((GEO >= '0500aa' && GEO <= '050355'))) || (((GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')))) && ((WKT >= '+AE0' && WKT < '+bE4'))";
        String expected = "((WKT >= '+AE0' && WKT < '+bE4') && ((GEO_WKT >= '0202\uDBFF\uDFFF+AE0' && GEO_WKT < '020d\uDBFF\uDFFF+bE4') || (GEO_WKT >= '030a\uDBFF\uDFFF+AE0' && GEO_WKT < '0335\uDBFF\uDFFF+bE4') || (GEO_WKT >= '0428\uDBFF\uDFFF+AE0' && GEO_WKT <= '0483\uDBFF\uDFFF+bE4') || (GEO_WKT >= '0500aa\uDBFF\uDFFF+AE0' && GEO_WKT < '050355\uDBFF\uDFFF+bE4') || (GEO_WKT >= '1f0aaaaaaaaaaaaaaa\uDBFF\uDFFF+AE0' && GEO_WKT < '1f36c71c71c71c71c7\uDBFF\uDFFF+bE4')))";

        runTestQuery(query, expected, indexedFields, conf);
    }

    @Test
    public void test24() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO_WKT", "GEO");
        compositeToFieldMap.put("GEO_WKT", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        conf.setFixedLengthFields(Arrays.asList("GEO"));

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO_WKT", 0));
        fieldToCompositeMap.put("WKT", new CompositeNameAndIndex("GEO_WKT", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "((((GEO >= '0202' && GEO <= '020d'))) || (((GEO >= '030a' && GEO <= '0335'))) || (((GEO >= '0428' && GEO <= '0483'))) || (((GEO >= '0500aa' && GEO <= '050355'))) || (((GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7'))))";
        String expected = "((((GEO >= '0202' && GEO <= '020d'))) || (((GEO >= '030a' && GEO <= '0335'))) || (((GEO >= '0428' && GEO <= '0483'))) || (((GEO >= '0500aa' && GEO <= '050355'))) || (((GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7'))))";

        runTestQuery(query, expected, indexedFields, conf);
    }

    @Test
    public void test25() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();

        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();

        compositeToFieldMap.put("GEO_WKT", "GEO");
        compositeToFieldMap.put("GEO_WKT", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);

        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");

        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = LinkedListMultimap.create();
        fieldToCompositeMap.put("GEO", new CompositeNameAndIndex("GEO_WKT", 0));
        fieldToCompositeMap.put("WKT", new CompositeNameAndIndex("GEO_WKT", 1));
        conf.setFieldToCompositeMap(fieldToCompositeMap);

        String query = "((((GEO >= '0202' && GEO <= '020d'))) || (((GEO >= '030a' && GEO <= '0335'))) || (((GEO >= '0428' && GEO <= '0483'))) || (((GEO >= '0500aa' && GEO <= '050355'))) || (((GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')))) && ((WKT >= '+AE0' && WKT < '+bE4'))";
        String expected = "((((GEO >= '0202' && GEO <= '020d'))) || (((GEO >= '030a' && GEO <= '0335'))) || (((GEO >= '0428' && GEO <= '0483'))) || (((GEO >= '0500aa' && GEO <= '050355'))) || (((GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')))) && ((WKT >= '+AE0' && WKT < '+bE4'))";

        runTestQuery(query, expected, indexedFields, conf);
    }

    void runTestQuery(String query, String expected) throws ParseException {
        runTestQuery(query, expected, INDEX_FIELDS, conf);
    }

    void runTestQuery(String query, String expected, Set<String> indexedFields, ShardQueryConfiguration conf) throws ParseException {
        ASTJexlScript original = JexlASTHelper.parseJexlQuery(query);
        ASTJexlScript expand = JexlASTHelper.parseJexlQuery(query);
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(indexedFields);

        expand = FunctionIndexQueryExpansionVisitor.expandFunctions(conf, helper, DateIndexHelper.getInstance(), expand);
        expand = ExpandCompositeTerms.expandTerms(conf, helper, expand);

        System.err.println(JexlStringBuildingVisitor.buildQuery(original));
        System.err.println(JexlStringBuildingVisitor.buildQuery(expand));

        String result = JexlStringBuildingVisitor.buildQuery(expand);
        Assert.assertEquals(result + " not equal to " + expected, expected, result);
    }

    void workIt(String query) throws Exception {
        System.err.println("incoming:" + query);
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        showIt("original", script);
        script = ExpandCompositeTerms.expandTerms(conf, null, script);
        showIt("expanded composites", script);
    }

    void showIt(String message, ASTJexlScript script) {
        System.err.println(message);
        PrintingVisitor.printQuery(script);
        System.err.println(JexlStringBuildingVisitor.buildQuery(script));
        script = TreeFlatteningRebuildingVisitor.flatten(script);
        System.err.println("flattened:");
        PrintingVisitor.printQuery(script);
        System.err.println(JexlStringBuildingVisitor.buildQuery(script));
        System.err.println();
    }
}
