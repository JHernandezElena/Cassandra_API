package edu.comillas.mibd;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;


public class ZA_Ejercicios {
    public static void main(String[] args){
        try (CqlSession session = CqlSession.builder()
                .withKeyspace(CqlIdentifier.fromCql("redfija"))
                .build()) {
            
        }

        System.out.println("FIN\n\n");

    }
}
