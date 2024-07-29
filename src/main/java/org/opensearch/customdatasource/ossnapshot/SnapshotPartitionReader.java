package org.opensearch.customdatasource.ossnapshot;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.json.JSONObject;
import org.opensearch.customdatasource.utils.SnapshotUtil;
import scala.collection.JavaConverters;

public class SnapshotPartitionReader implements PartitionReader<InternalRow> {
    private final IndexReader indexReader;
    private final IndexSearcher indexSearcher;
    private final Iterator<ScoreDoc> scoreDocs;
    private final StructType schema;
    private final SnapshotParams snapshotParams;
    private final List<BiFunction> valueConverters;

    public SnapshotPartitionReader(SnapshotParams snapshotParams, StructType schema, SnapshotInputPartition snapshotInputPartition) throws IOException {
        this.snapshotParams = snapshotParams;
        Directory directory = SnapshotUtil.getRemoteSnapShotDirectory(snapshotParams, snapshotInputPartition.getSnapshotUUID(),
            snapshotInputPartition.getIndexId(),
            snapshotInputPartition.getShardId());
        this.indexReader = DirectoryReader.open(directory);
        this.indexSearcher = new IndexSearcher(indexReader);
        TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
        this.scoreDocs = Arrays.asList(topDocs.scoreDocs).iterator();
        this.schema = schema;
        this.valueConverters = ValueConverters.getJsonConverters(schema);
        System.out.println("Partition Num:" + snapshotInputPartition.getShardId());
    }

    @Override
    public boolean next() {
        return scoreDocs.hasNext();
    }

    @Override
    public InternalRow get() {
        try {
            ScoreDoc scoreDoc = scoreDocs.next();
            Document doc = indexSearcher.doc(scoreDoc.doc);
            return convertToInternalRow(doc);
        } catch (IOException e) {
            throw new NoSuchElementException("Failed to retrieve next document: " + e.getMessage());
        }
    }

    private InternalRow convertToInternalRow(Document doc) {
        BytesRef sourceBytes = doc.getBinaryValue("_source");
        JSONObject sourceJson = new JSONObject(new String(sourceBytes.bytes, StandardCharsets.UTF_8));

        Object[] values = new Object[schema.fields().length];
        for (int i = 0; i < schema.fields().length; i++) {
            String fieldName = schema.fields()[i].name();
            values[i] = valueConverters.get(i).apply(sourceJson, fieldName);
        }
        return InternalRow.fromSeq(
            JavaConverters.asScalaIteratorConverter(Arrays.asList(values).iterator()).asScala().toSeq());
    }

    @Override
    public void close() throws IOException {
        if (indexReader != null) {
            System.out.println("Close: 1");
            indexReader.close();
        }
    }
}
