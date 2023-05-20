import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.util.Arrays.asList;

public class Repro {
  private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

  public static void main(String[] args) throws IOException, SQLException {
    try (VectorSchemaRoot table = createIntervalTable();
         ArrowReader reader = toReader(table);
         ArrowArrayStream stream = ArrowArrayStream.allocateNew(ALLOCATOR)) {
      Data.exportArrayStream(ALLOCATOR, reader, stream);
      try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
        conn.registerArrowStream("bad", stream);
        try (PreparedStatement stmt = conn.prepareStatement("select count(*) from bad");
             ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            System.out.println(rs.getInt(1));
          }
        }
      }
    }
  }

  private static VectorSchemaRoot createIntervalTable() {
    Field interval = new Field("interval",
        FieldType.notNullable(new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO)),
        /* children */ null);
    Schema schema = new Schema(asList(interval));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, ALLOCATOR);
    IntervalMonthDayNanoVector vector = (IntervalMonthDayNanoVector) root.getVector(0);
    root.setRowCount(1);
    vector.allocateNew(1);
    vector.set(0, 0, 0, 0);
    return root;
  }

  private static ArrowReader toReader(VectorSchemaRoot root) throws IOException {
    try (root) {
      ByteArrayOutputStream ipc = new ByteArrayOutputStream();
      try (ArrowWriter writer = new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider(), ipc)) {
        writer.writeBatch();
      }
      return new ArrowStreamReader(new ByteArrayInputStream(ipc.toByteArray()), ALLOCATOR);
    }
  }
}
