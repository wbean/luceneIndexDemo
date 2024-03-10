package wang.yongbin;

import jakarta.annotation.Resource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;


@SpringBootTest
public class LuceneIndexServiceTest {

    @Resource
    private LuceneIndexService luceneIndexService;

    public LuceneIndexServiceTest() throws IOException {
    }

    @ParameterizedTest()
    @CsvSource(value = {
            "1,NULL,NULL,NULL,NULL,1",
            "NULL,测试二,NULL,NULL,NULL,2",
            "NULL,NULL,FINISH|FAIL,NULL,NULL,3",
            "NULL,NULL,NULL,2021-01-01 00:00:00,2021-01-01 00:00:00,0",
            "NULL,NULL,NULL,2024-01-04 15:15:04,2024-01-06 15:15:04,2",
    }, nullValues = "NULL")
    void testSearch(Long id, String title, String status, String startTime, String endTime, int expectRowCount) throws IOException, ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<LuceneIndexService.DocData> search = luceneIndexService.search(
                LuceneIndexService.SearchParam.builder()
                        .id(id)
                        .title(title)
                        .statuses(status == null ? null : List.of(status.split("\\|")))
                        .startTime(startTime == null ? null : simpleDateFormat.parse(startTime).getTime())
                        .endTime(endTime == null ? null : simpleDateFormat.parse(endTime).getTime())
                        .build());
        Assertions.assertEquals(expectRowCount, search.size());
    }

    @ParameterizedTest()
    @CsvSource(value = {
            "2024-01-01 00:00:00, 2024-02-01 00:00:00, 1, 5, 5",
            "2024-01-01 00:00:00, 2024-02-01 00:00:00, 2, 5, 1",
    }, nullValues = "NULL")
    void testSearchPagination(String startTime, String endTime, int page, int pageSize, int expectRowCount) throws ParseException, IOException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<LuceneIndexService.DocData> search = luceneIndexService.search(
                LuceneIndexService.SearchParam.builder()
                        .startTime(startTime == null ? null : simpleDateFormat.parse(startTime).getTime())
                        .endTime(endTime == null ? null : simpleDateFormat.parse(endTime).getTime())
                        .page(page)
                        .pageSize(pageSize)
                        .build());
        Assertions.assertEquals(expectRowCount, search.size());
    }

    @ParameterizedTest()
    @CsvSource(value = {
            "2024-01-01 00:00:00, 2024-02-01 00:00:00, ASC, 1",
            "2024-01-01 00:00:00, 2024-02-01 00:00:00, DESC, 6",
    }, nullValues = "NULL")
    void testIdSortSearch(String startTime, String endTime, LuceneIndexService.SearchParam.Sort sort, int expectFirstRowId) throws IOException, ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<LuceneIndexService.DocData> search = luceneIndexService.search(
                LuceneIndexService.SearchParam.builder()
                        .startTime(startTime == null ? null : simpleDateFormat.parse(startTime).getTime())
                        .endTime(endTime == null ? null : simpleDateFormat.parse(endTime).getTime())
                        .sortById(sort)
                        .build());
        Assertions.assertEquals(6, search.size());
        Assertions.assertEquals(expectFirstRowId, search.get(0).getId());
    }

    @ParameterizedTest()
    @CsvSource(value = {
            "1,NULL,NULL,NULL,NULL,1",
            "NULL,测试二,NULL,NULL,NULL,2",
            "NULL,NULL,FINISH|FAIL,NULL,NULL,3",
            "NULL,NULL,NULL,2021-01-01 00:00:00,2021-01-01 00:00:00,0",
            "NULL,NULL,NULL,2024-01-04 15:15:04,2024-01-06 15:15:04,2",
    }, nullValues = "NULL")
    void testSearchCount(Long id, String title, String status, String startTime, String endTime, int expectRowCount) throws IOException, ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        int total = luceneIndexService.count(
                LuceneIndexService.SearchParam.builder()
                        .id(id)
                        .title(title)
                        .statuses(status == null ? null : List.of(status.split("\\|")))
                        .startTime(startTime == null ? null : simpleDateFormat.parse(startTime).getTime())
                        .endTime(endTime == null ? null : simpleDateFormat.parse(endTime).getTime())
                        .build());
        Assertions.assertEquals(expectRowCount, total);
    }

    @ParameterizedTest
    @CsvSource({
            "1,有限的标题测试,INIT,2024-01-01 12:12:01"
            })
    void testUpdateIndex(Long id, String title, String status, String time) throws ParseException, IOException, InterruptedException {
        long currentTimeMillis = System.currentTimeMillis();
        System.out.printf("currentTimeMillis: %s\n", currentTimeMillis);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String updatedTitle = title + currentTimeMillis;
        LuceneIndexService.DocData docData = LuceneIndexService.DocData.builder()
                .id(id)
                .title(updatedTitle)
                .status(status)
                .time(simpleDateFormat.parse(time).getTime())
                .build();

        luceneIndexService.updateIndex(List.of(docData));

        // wait for data flush and refresh
        Thread.sleep(6000);
        List<LuceneIndexService.DocData> search = luceneIndexService.search(
                LuceneIndexService.SearchParam.builder()
                        .id(id)
                        .build());

        System.out.printf("search: %s\n", search);

        Assertions.assertEquals(1, search.size());
        Assertions.assertEquals(updatedTitle, search.get(0).getTitle());

        // Revert the change
        docData.setTitle(title);
        luceneIndexService.updateIndex(List.of(docData));
    }
}
