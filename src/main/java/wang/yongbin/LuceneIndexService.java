package wang.yongbin;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.MMapDirectory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;


@Slf4j
@Component
public class LuceneIndexService implements DisposableBean {
    public static final String ID = "id";
    public static final String TITLE = "title";
    public static final String STATUS = "status";
    public static final String TIME = "time";
    private final IndexWriter writer;
    private IndexSearcher searcher;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // refresh IndexSearcher every 5 seconds
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void destroy() throws Exception {
        executorService.shutdown();
        writer.close();
    }


    @Data
    @Builder
    public static class DocData {
        private Long documentId;
        private Long id;
        private String title;
        private String status;
        private Long time;
    }

    @Data
    @Builder
    public static class SearchParam {
        public enum Sort {
            ASC, DESC
        }

        private Long id;
        private String title;
        private List<String> statuses;
        private Long startTime;
        private Long endTime;

        private int page;
        private int pageSize;

        private Sort sortById;
    }

    public enum Status {
        INIT, FINISH, SUCCESS, FAIL
    }

    /**
     * if index not exists, create a new index and initialize index data
     * if index exists, open existing index in append mode
     * @throws IOException will be thrown if failed to create index, stop the application
     */
    public LuceneIndexService() throws IOException {
        Path indexPath = Paths.get("/tmp/index/data");
        MMapDirectory directory = new MMapDirectory(indexPath);
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);

        if (!DirectoryReader.indexExists(directory)) {
            // create a new index if not exists
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            writer = new IndexWriter(directory, config);
            // initialize index data
            initIndex();
        } else {
            // open existing index in append mode if exists
            config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
            writer = new IndexWriter(directory, config);
        }

        // create IndexSearcher
        IndexReader reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);

        // refresh IndexSearcher every 5 seconds
        executorService.scheduleAtFixedRate(() -> {
            try {
                refreshSearcher();
            } catch (IOException e) {
                log.error("Failed to refresh index searcher", e);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * initialize index data
     * @throws IOException will be thrown if failed to initialize index data
     */
    private void initIndex() throws IOException {
        // load data from CSV file
        List<DocData> docDataList = loadDataFromCSVFile();
        rwLock.writeLock().lock();
        try {
            // foreach DocData, create a Document
            for (DocData data : docDataList) {
                Document doc = createDocument(data);
                // add Document to index
                writer.addDocument(doc);
            }
            // flush and commit
            writer.flush();
            writer.commit();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * load data from CSV file
     * @return a list of DocData objects
     * @throws IOException will be thrown if failed to load data from CSV file
     */
    private List<DocData> loadDataFromCSVFile() throws IOException {
        String EXAMPLE_DATA_FILE = "data.csv";
        Resource resource = new ClassPathResource(EXAMPLE_DATA_FILE);
        InputStream inputStream = resource.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        List<DocData> data = new ArrayList<>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // read CSV data file in rows
        String line;
        while ((line = reader.readLine()) != null) {
            // split row by comma
            String[] values = line.split(",");

            // check if the row has the correct number of columns
            int EXAMPLE_DATA_FILE_COLUMN_LENGTH = 4;
            if (values.length != EXAMPLE_DATA_FILE_COLUMN_LENGTH) {
                log.warn("Illegal data schemer:{}", line);
                continue;
            }

            // validate and parse each column
            long id;
            try {
                id = Long.parseLong(values[0]);
            } catch (Exception e) {
                log.warn("Illegal id format, id={}", values[0]);
                continue;
            }

            Status status;
            try {
                status = Status.valueOf(values[2]);
            } catch (Exception e) {
                log.warn("Illegal status format, status={}", values[2]);
                continue;
            }

            long time;
            try {
                time = dateFormat.parse(values[3]).getTime();
            } catch (Exception e) {
                log.warn("Illegal time format, time={}", values[3]);
                continue;
            }

            // create a DocData object
            data.add(DocData.builder()
                    .id(id)
                    .title(values[1])
                    .status(status.name())
                    .time(time)
                    .build());
        }
        return data;
    }

    /**
     * add / update documents to index
     * @param docDataList a list of DocData objects
     * @return the number of documents that added/updated
     */
    public int updateIndex(List<DocData> docDataList) {
        List<Document> docs;
        try{
             docs = docDataList.stream()
                    .map(this::createDocument)
                    .toList();
        }catch (Exception e){
            log.error("Failed to create document from data, param invalid", e);
            throw new RuntimeException(e);
        }

        List<Long> ids = docDataList.stream()
                .map(DocData::getId)
                .toList();

        rwLock.writeLock().lock();
        try {
            // update documents
            writer.updateDocuments(LongPoint.newSetQuery("id", ids), docs);
            writer.flush();
            return ids.size();
        } catch (IOException e) {
            log.info("Failed to update index, param={}", docDataList, e);
            throw new RuntimeException(e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * search documents that match the searchParam
     * @param searchParam SearchParam object
     * @return a list of documents that match the searchParam
     */
    public List<DocData> search(SearchParam searchParam){
        int page = searchParam.getPage();
        int pageSize = searchParam.getPageSize();
        if (page < 1) {
            page = 1;
        }
        if (pageSize < 1) {
            pageSize = 10;
        }

        int start = (page - 1) * pageSize;
        int topN = page * pageSize;

        // get Sort object from SearchParam
        Sort sort = getSort(searchParam);

        // build Lucene Query from SearchParam
        Query query = buildQuery(searchParam);

        rwLock.readLock().lock();
        try {
            // search index
            TopDocs topDocs = searcher.search(query, topN, sort);
            StoredFields storedFields = searcher.storedFields();

            // get documents from TopDocs
            return Arrays.stream(topDocs.scoreDocs)
                    .filter(hit -> hit.doc >= start)
                    .map(hit -> {
                        Document doc;
                        try {
                            doc = storedFields.document(hit.doc);
                        } catch (IOException e) {
                            log.error("Failed to get document from index", e);
                            return null;
                        }
                        return DocData.builder()
                                .id(Long.parseLong(doc.get(ID)))
                                .title(doc.get(TITLE))
                                .status(doc.get(STATUS))
                                .time(Long.parseLong(doc.get(TIME)))
                                .build();
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("Failed to search index, param={}", searchParam, e);
            throw new RuntimeException(e);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * get Sort object from SearchParam
     * @param searchParam SearchParam object
     * @return Sort object
     */
    private static Sort getSort(SearchParam searchParam) {
        Sort sort = Sort.RELEVANCE;
        if (searchParam.getSortById() != null) {
            SortField sortField = new SortField("id", SortField.Type.LONG);
            if (searchParam.getSortById() == SearchParam.Sort.DESC) {
                sortField = new SortField("id", SortField.Type.LONG, true);
            }
            sort = new Sort(sortField);
        }
        return sort;
    }

    /**
     * count the number of documents that match the searchParam
     * @param searchParam SearchParam object
     * @return the number of documents that match the searchParam
     */
    public int count(SearchParam searchParam) {
        Query query = buildQuery(searchParam);
        rwLock.readLock().lock();
        try {
            return searcher.count(query);
        } catch (IOException e) {
            log.error("Failed to count index, param={}", searchParam, e);
            throw new RuntimeException(e);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * build a Lucene Query from a SearchParam object
     * @param searchParam SearchParam object
     * @return Lucene Query
     */
    private Query buildQuery(SearchParam searchParam) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        // id support exact match
        if (searchParam.getId() != null) {
            builder.add(LongPoint.newExactQuery("id", searchParam.getId()), BooleanClause.Occur.MUST);
        }

        // title support fuzzy search
        if (searchParam.getTitle() != null) {
            builder.add(new FuzzyQuery(new Term("title", searchParam.getTitle()), 2), BooleanClause.Occur.SHOULD);
        }

        // status support multi-value search
        if (searchParam.getStatuses() != null && !searchParam.getStatuses().isEmpty()) {
            BooleanQuery.Builder statusBuilder = new BooleanQuery.Builder();
            for (String status : searchParam.getStatuses()) {
                statusBuilder.add(new TermQuery(new Term("status", status)), BooleanClause.Occur.SHOULD);
            }
            builder.add(statusBuilder.build(), BooleanClause.Occur.MUST);
        }

        // time support range search
        if (searchParam.getStartTime() != null && searchParam.getEndTime() != null) {
            builder.add(LongPoint.newRangeQuery("time", searchParam.getStartTime(), searchParam.getEndTime()), BooleanClause.Occur.MUST);
        }

        return builder.build();
    }


    /**
     * create a Lucene Document from a DocData object
     * @param data DocData object
     * @return Lucene Document
     */
    private Document createDocument(DocData data) {
        // validate data
        if(!validateData(data)){
            throw new RuntimeException("Illegal docData: " + data);
        }
        Document doc = new Document();
        long idValue = data.getId();
        doc.add(new LongPoint("id", idValue));
        doc.add(new StoredField("id", idValue));
        doc.add(new NumericDocValuesField("id", idValue));
        doc.add(new TextField("title", data.getTitle(), Field.Store.YES));
        doc.add(new StringField("status", data.getStatus(), Field.Store.YES));
        doc.add(new LongField("time", data.getTime(), Field.Store.YES));
        return doc;
    }

    /**
     * validate DocData object
     * @param data DocData object
     * @return true if the DocData object is valid, otherwise false
     */
    private boolean validateData(DocData data) {
        if (data.getId() == null || data.getTitle() == null || data.getStatus() == null || data.getTime() == null) {
            return false;
        }

        if (data.getId() < 0 || data.getTime() < 0) {
            return false;
        }

        try{
            Status.valueOf(data.getStatus());
        }catch (Exception e){
            return false;
        }

        return true;
    }


    /**
     * refresh IndexSearcher
     * @throws IOException will be thrown if failed to refresh IndexSearcher
     */
    private void refreshSearcher() throws IOException {
        rwLock.readLock().lock();
        try {
            if (searcher != null) {
                searcher.getIndexReader().close();
            }
            searcher = new IndexSearcher(DirectoryReader.open(writer));
        } finally {
            rwLock.readLock().unlock();
        }
    }
}