package lucene;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.customdatasource.lucene.LuceneFileSearch;

public class LuceneFileSearchIntegrationTest {

    @Test
    public void givenSearchQueryWhenFetchedFileNameIsCorrect() throws IOException {
        LuceneFileSearch luceneFileSearch = new LuceneFileSearch();
        List<Document> docs = luceneFileSearch.searchFiles();
        Assertions.assertEquals(1000, docs.size());
    }

}