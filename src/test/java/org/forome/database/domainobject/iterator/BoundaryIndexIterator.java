package org.forome.database.domainobject.iterator;

//
//@ExtendWith(PowerMockExtension.class)
//@PrepareForTest(BaseIndex.class)
//public class BoundaryIndexIterator extends DomainDataTest {
//
//    @Before
//    public void setUp() throws Exception {
//        PowerMockito.spy(BaseIndex.class);
//        AtomicInteger atomicInteger = new AtomicInteger(0);
//        PowerMockito.when(BaseIndex.class, "buildFieldsHashCRC32", ArgumentMatchers.anyList())
//                .then(h -> TypeConvert.pack(atomicInteger.getAndIncrement()));
//
//        createDomain(BoundaryEditable.class);
//    }
//
//    @Test
//    public void testBoundaryIndex() throws Exception {
//        fillData(domainObjectSource);
//
//        //Hash
//        assertFilteringResult(new HashFilter(BoundaryEditable.FIELD_LONG_1, 2L),
//                Arrays.asList(1L, 2L, 3L));
//
//        assertFilteringResult(new HashFilter(BoundaryEditable.FIELD_LONG_2, 2L),
//                Arrays.asList(4L, 5L, 6L));
//
//
//        //Interval
//        assertFilteringResult(new IntervalFilter(BoundaryEditable.FIELD_LONG_1, Long.MIN_VALUE, Long.MAX_VALUE),
//                Arrays.asList(4L, 5L, 6L, 1L, 2L, 3L));
//
//        assertFilteringResult(new IntervalFilter(BoundaryEditable.FIELD_LONG_2, Long.MIN_VALUE, Long.MAX_VALUE),
//                Arrays.asList(4L, 5L, 6L, 1L, 2L, 3L));
//
//        assertFilteringResult(new IntervalFilter(BoundaryEditable.FIELD_LONG_3, Long.MIN_VALUE, Long.MAX_VALUE),
//                Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L));
//
//
//        assertFilteringResult(new IntervalFilter(BoundaryEditable.FIELD_LONG_1, 2L, 3L),
//                Arrays.asList(1L, 2L, 3L));
//
//        assertFilteringResult(new IntervalFilter(BoundaryEditable.FIELD_LONG_2, 2L, 3L),
//                Arrays.asList(4L, 5L, 6L));
//
//
//        assertFilteringResult(new IntervalFilter(BoundaryEditable.FIELD_LONG_1, 2L, 2L),
//                Arrays.asList(1L, 2L, 3L));
//
//        assertFilteringResult(new IntervalFilter(BoundaryEditable.FIELD_LONG_2, 2L, 2L),
//                Arrays.asList(4L, 5L, 6L));
//
//
//        //Prefix
//        assertFilteringResult(new PrefixFilter(BoundaryEditable.FIELD_STRING_1, "a"),
//                Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L));
//
//        assertFilteringResult(new PrefixFilter(BoundaryEditable.FIELD_STRING_1, "ab"),
//                Arrays.asList(4L, 5L, 6L));
//
//        assertFilteringResult(new PrefixFilter(BoundaryEditable.FIELD_STRING_2, "ab"),
//                Arrays.asList(1L, 2L, 3L));
//
//        assertFilteringResult(new PrefixFilter(BoundaryEditable.FIELD_STRING_2, "ae"),
//                Arrays.asList(4L, 5L, 6L));
//
//        assertFilteringResult(new PrefixFilter(BoundaryEditable.FIELD_STRING_3, "ae"),
//                Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L));
//    }
//
//    @Test
//    public void testBoundaryIndexRange() throws Exception {
//        fillDataForRange(domainObjectSource);
//
//        //Range
//        assertFilteringResult(new RangeFilter(BoundaryEditable.RANGE_LONG_FIELD, Long.MIN_VALUE, Long.MAX_VALUE),
//                Arrays.asList(1L, 2L, 3L, 4L));
//
//        assertFilteringResult(new RangeFilter(BoundaryEditable.RANGE_LONG_FIELD_2, Long.MIN_VALUE, Long.MAX_VALUE),
//                Arrays.asList(1L, 2L, 3L, 4L));
//    }
//
//    private <T extends Filter> void assertFilteringResult(T filter, List<Long> expectedIds) throws Exception {
//        List<Long> actual = new ArrayList<>();
//        domainObjectSource.executeTransactional(transaction -> {
//            try(IteratorEntity<BoundaryEditable> it = transaction.find(BoundaryEditable.class, filter)) {
//                while (it.hasNext()) {
//                    actual.add(it.next().getId());
//                }
//            }
//        });
//        Assert.assertEquals(expectedIds, actual);
//    }
//
//    private void fillData(DomainObjectSource domainObjectSource) throws Exception {
//        domainObjectSource.executeTransactional(transaction -> {
//            for (int i = 0; i < 3; i++) {
//                BoundaryEditable obj = transaction.create(BoundaryEditable.class);
//                obj.setLong1(2L);
//                obj.setLong2(4L);
//                obj.setLong3(5L);
//                obj.setString1("a");
//                obj.setString2("abc");
//                obj.setString3("ae");
//                transaction.save(obj);
//            }
//            for (int i = 0; i < 3; i++) {
//                BoundaryEditable obj = transaction.create(BoundaryEditable.class);
//                obj.setLong1(1L);
//                obj.setLong2(2L);
//                obj.setLong3(6L);
//                obj.setString1("ab");
//                obj.setString2("ae");
//                obj.setString3("aet");
//                transaction.save(obj);
//            }
//        });
//    }
//
//    private void fillDataForRange(DomainObjectSource domainObjectSource) throws Exception {
//        domainObjectSource.executeTransactional(transaction -> {
//            for (int i = 0; i < 2; i++) {
//                BoundaryEditable obj = transaction.create(BoundaryEditable.class);
//                obj.setLong1(1L);
//                obj.setLong2(4L);
//                obj.setLong3(6L);
//                transaction.save(obj);
//            }
//            for (int i = 0; i < 2; i++) {
//                BoundaryEditable obj = transaction.create(BoundaryEditable.class);
//                obj.setLong1(2L);
//                obj.setLong2(5L);
//                obj.setLong3(7L);
//                transaction.save(obj);
//            }
//        });
//    }
//}
