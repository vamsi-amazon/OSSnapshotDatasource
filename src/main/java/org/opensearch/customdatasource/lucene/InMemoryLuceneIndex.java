package org.opensearch.customdatasource.lucene;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

public class InMemoryLuceneIndex {

  private Directory directory;
  private Analyzer analyzer;

  public InMemoryLuceneIndex(Directory directory, Analyzer analyzer) {
    this.directory = directory;
    this.analyzer = analyzer;
  }

  public void addIndexDocument(String title, String body) {
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
    try {
      IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
      Document document = new Document();
      document.add(new TextField("title", title, Field.Store.YES));
      document.add(new TextField("body", body, Field.Store.YES));
      document.add(new SortedDocValuesField("title", new BytesRef(title)));
      indexWriter.addDocument(document);
      indexWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public List<Document> searchIndex(String inField, String queryString) {
    try {
      Query query = new QueryParser(inField, analyzer).parse(queryString);
      IndexReader indexReader = DirectoryReader.open(directory);
      IndexSearcher indexSearcher = new IndexSearcher(indexReader);
      TopDocs topDocs = indexSearcher.search(query, 10);
      StoredFields storedFields = indexSearcher.storedFields();
      List<Document> documents = new ArrayList<>();
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        documents.add(storedFields.document(scoreDoc.doc));
      }
      return documents;
    } catch (ParseException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void deleteDocument(Term term) {
    try {
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
      IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
      indexWriter.deleteDocuments(term);
      indexWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  public List<Document> searchIndex(Query query) {
    try {
      IndexReader indexReader = DirectoryReader.open(directory);
      IndexSearcher indexSearcher = new IndexSearcher(indexReader);
      TopDocs topDocs = indexSearcher.search(query, 10);
      StoredFields storedFields = indexSearcher.storedFields();
      List<Document> documents = new ArrayList<>();
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        documents.add(storedFields.document(scoreDoc.doc));
      }
      return documents;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  public List<Document> searchIndex(Query query, Sort sort) {
    try {
      IndexReader indexReader = DirectoryReader.open(directory);
      IndexSearcher indexSearcher = new IndexSearcher(indexReader);
      TopDocs topDocs = indexSearcher.search(query, 10, sort);
      StoredFields storedFields = indexSearcher.storedFields();
      List<Document> documents = new ArrayList<>();
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        documents.add(storedFields.document(scoreDoc.doc));
      }
      return documents;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


}
