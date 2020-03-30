package edu.comillas.mibd;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;

import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

public class BD_DeleteExample {
    public static void main(String[] args){
        try (CqlSession session = CqlSession.builder()
                .withKeyspace(CqlIdentifier.fromCql("redfija"))
                .build()) {


            //se utiliza un builder para borra sólo las columnas tz e id de las filas que cumplen la condición
            Delete delete = deleteFrom("lectura_hash")
                    .column("tz")
                    .column("id")
                    .whereColumn("hash").isEqualTo(literal("2#C##PJELR00000001#"))
                    .whereColumn("am").isEqualTo(literal(201709))
                    .whereColumn("fecha").isEqualTo(literal("2017-09-01 12:00:00"));

            //Se ejecuta la instrucción
            ResultSet deleteResult = session.execute(delete.build());
            //Se muestra el resultado
            System.out.println(deleteResult.wasApplied()); //no itero porque solo devuelve si se ha aplicado o no


            //Se utiliza un builder para borra todas las filas que cumplen las condición
            Delete delete2 = deleteFrom("lectura_hash")
                    .whereColumn("hash").isEqualTo(literal("2#C##PJELR00000001#"))
                    .whereColumn("am").isEqualTo(literal(201709))
                    .whereColumn("fecha").isEqualTo(literal("2017-09-01 12:00:00"))
                    .ifColumn("valida").isEqualTo(literal(false));

            //se ejecuta la instrucción
            ResultSet deleteResult2 = session.execute(delete2.build());
            //Se muestra el resultado
            System.out.println(deleteResult2.wasApplied());


            //Se utiliza un builder para borra todas las filas que cumplen las condición
            Delete delete3 = deleteFrom("lectura_hash")
                    .whereColumn("hash").isEqualTo(literal("2#C##PJELR00000001#"))
                    .whereColumn("am").isEqualTo(literal(201709))
                    .whereColumn("fecha").isEqualTo(literal("2017-09-01 12:00:00"));


            //se ejecuta la instrucción
            ResultSet deleteResult3 = session.execute(delete3.build());
            //Se muestra el resultado
            System.out.println(deleteResult3.wasApplied());
        }

        System.out.println("FIN\n\n");

    }
}
