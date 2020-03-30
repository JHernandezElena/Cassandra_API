package edu.comillas.mibd;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import java.util.UUID;

public class BC_WriteExample {
    public static void main(String[] args){

        try (CqlSession session = CqlSession.builder()
                .withKeyspace(CqlIdentifier.fromCql("redfija"))
                .build()) {

            //Se genera un identificador universal
            UUID id = UUID.randomUUID();

            //Se utiliza una clase builder de inserción
            RegularInsert insert = insertInto("lectura_hash")
                    .value("id", literal(id))
                    .value("hash", literal("2#C##PJELR00000001#"))
                    .value("am", literal(201709))
                    .value("fecha", literal("2017-09-01 12:00:00"))
                    .value("tz", literal("Europe/Madrid"))
                    .value("valor", literal(16000))
                    .value("precis", literal("m3"))
                    .value("origen", literal(5))
                    .value("metodo", literal(15))
                    .value("valida", literal(true))
                    .value("usuc", literal("PJEL"))
                    .value("fecc", literal("2017-07-01 00:00:00"))
                    .value("del", literal(false));

            //Se ejecuta la instrucción
            ResultSet lecturaInsertResult = session.execute(insert.build());

            //Se muestra el resultado de la ejecución
            System.out.println(lecturaInsertResult.wasApplied()); //no itero porque solo devuelvo si se ha aplicado o no
            System.out.println(lecturaInsertResult.getExecutionInfo().getIncomingPayload());
        }

        System.out.println("FIN\n\n");
    }
}
