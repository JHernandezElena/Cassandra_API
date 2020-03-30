package edu.comillas.mibd;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

public class BE_UpdateExample {
    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder()
                .withKeyspace(CqlIdentifier.fromCql("redfija"))
                .build()) {

            //OJO hay que utilizar where para toda la clave primaria. Solo se puede usar igualdad o el operador IN
            //OJO update no comprueba si existe. Si no existe lo crea
            Update up = update("lectura_hash")
                    .setColumn("metodo",literal(16))
                    .setColumn("usuc",literal("PEMA2"))
                    .whereColumn("hash").isEqualTo(literal("2#C##PJELR00000001#"))
                    .whereColumn("am").isEqualTo(literal(201709)) //SI HACEMOS TAB HAY MUCHOS MAS COMPARADORES
                    .whereColumn("fecha").isEqualTo(literal("2017-09-01 12:00:00"))
                    .ifColumn("valida").isEqualTo(literal(true))
                    .ifExists();


            SimpleStatement upStatment = up.build();
            //Se ejecuta el update
            ResultSet upResult = session.execute(upStatment);
            //se visualiza el estado de la ejecución
            System.out.println(upResult.wasApplied());
            System.out.println();

            //Se hace una consulta para verificar la ejecución
            Select select = selectFrom("lectura_hash")
                    .all()
                    .whereColumn("usuc").isEqualTo(literal("PEMA2"));

            //Se ejecuta la consulta
            ResultSet selectResult = session.execute(select.build());
            //Se visualizan los resultados
            for (Row row : selectResult) {
                //Se muestra por consola información de cada registro
                System.out.format("hash: %s, fecha: %s, valor: %s, valida: %s, metodo: %d, usuc: %s\n",
                        row.getString("hash"),row.getInstant("fecha"),row.getInt("valor"), row.getBoolean("valida"), row.getInt("metodo"),row.getString("usuc"));
            }

        }

        System.out.println("FIN\n\n");
    }
}
