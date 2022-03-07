package io.substrait.isthmus;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.substrait.proto.Plan;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

public class SimplePlansTest extends PlanTestBase {

  @Test
  public void aggFilter() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    s.execute("select sum(L_ORDERKEY) filter(WHERE L_ORDERKEY > 10) from lineitem ", creates);
  }

  @Test
  public void cd() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    //creates.forEach(System.out::println);
    s.execute("select sum(distinct L_ORDERKEY) from lineitem ", creates);
  }

  @Test
  public void filter() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    //creates.forEach(System.out::println);
    print(s.execute("select * from lineitem WHERE L_ORDERKEY > 10", creates));
  }

    @Test
    public void sql1() throws IOException, SqlParseException {
        SqlToSubstrait s = new SqlToSubstrait();  // the default functions have been loaded into s.EXTENSION_COLLECTION
        String[] values = asString("tpch/schema.sql").split(";");
        var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
        // remove the blank part
        // "tpch/schema.sql" is separated into several parts, where the last one is "\n"
        // the last part should be removed

        //creates.forEach(System.out::println);
        print(s.execute("select sum(l_suppkey+l_partkey) from lineitem where l_orderkey > 10", creates));  // input: sql_command, tables_from_schema.sql
    }


  private void print(Plan plan) {
    try {
      System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }


}
