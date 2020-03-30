package edu.comillas.mibd;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.Selector;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;


public class BB_ReadExample {
    public static void main(String[] args){

        //CASO 1: Ejecutar directamente una sentencia SQL
        CqlSession session = CqlSession.builder()
                .withKeyspace(CqlIdentifier.fromCql("redfija"))
                .build();
        String query = "SELECT * FROM lectura_hash WHERE hash = '2#C##PJELR00000001#'";
        ResultSet rs = session.execute(query);

        //Se visualiza el resultado de la ejecución
        for (Row row : rs) {
            //Se muestra por consola información de cada registro
            System.out.format("id: %s, hash: %s, valor: %s, fecha:%s\n", row.getUuid("id"),
                    row.getString("hash"), row.getInt("Valor"), row.getInstant("fecha"));
        }



        //CASO 2: Utilizando una clase Bulder para construir la sentencia
                //PARA TENER UNA CONSULTA ESTRUCTURADA
                //MEJOR PARA COMPROBAR ERROR EN COMPILACION
        Select select = selectFrom("lectura_hash")
                .column("fecha")
                .column("valor")
                .column("valida")
                .whereColumn("hash").isEqualTo(literal("2#C##PJELR00000001#"))
                .orderBy("am", ClusteringOrder.ASC)
                .limit(10);

        //Se construye la sentencia
        SimpleStatement selectStatment = select.build();
        //Se ejecuta la instrucción
        ResultSet selectResult = session.execute(selectStatment);
        //Se visualiza los resultados
        for (Row row : selectResult) {
            //Se muestra por consola información de cada registro
            System.out.format("fecha: %s, valor: %s, valida: %s\n", row.getInstant("fecha"),
                    row.getInt("valor"), row.getBoolean("valida"));
        }

        System.out.println();

        //CASO 3: Otra sintaxis para hacer el CASO 2
        Select select2 = selectFrom("lectura_hash")
                .columns("fecha","valor","valida") //PARA PASAR TODAS LAS COLUMNAS DE GOLPE
                .whereColumn("hash").isEqualTo(literal("2#C##PJELR00000001#"))
                .orderBy("am", ClusteringOrder.ASC)
                .limit(10);

        selectStatment = select2.build();
        ResultSet selectResult2 = session.execute(selectStatment);
        for (Row row : selectResult2) {
            //Se muestra por consola información de cada registro
            System.out.format("fecha: %s, valor: %s, valida: %s\n", row.getInstant("fecha"),
                    row.getInt("valor"), row.getBoolean("valida"));
        }


        //CASO 4: Otra sintaxix y se utiliza los valores ttl y tiempo de actualización
        Select select3 = selectFrom("lectura_hash")
                .selectors(
                        Selector.column("fecha"),
                        Selector.column("valor"),
                        Selector.column("valida"),
                        Selector.ttl("valida").as("ttl"),
                        Selector.column("precis").as("precision"),
                        Selector.writeTime("precis").as("WriteTime")
                ).whereColumn("hash").isEqualTo(literal("2#C##PJELR00000001#"))
                .orderBy("am", ClusteringOrder.ASC)
                .limit(10);

        //Se visualizan los resultados
        ResultSet selectResult3 = session.execute(select3.build());
        for (Row row : selectResult3) {
            //Se muestra por consola información de cada registro
            System.out.format("fecha: %s, valor: %s, valida: %s, ttl: %d, precisión: %s, wt: %d\n", row.getInstant("fecha"),
                    row.getInt("valor"), row.getBoolean("valida"), row.getInt("ttl"),row.getString("precision"),row.getLong("WriteTime"));
        }




        //CASO 5: se cuentan los registros de la tabla
        Select select4 = selectFrom("lectura_hash").countAll().as("cuenta")
                .whereColumn("hash").isEqualTo(literal("2#C##PJELR00000001#"));

        SimpleStatement ss4 = select4.build();

        //Se visualizan los resultados
        ResultSet selectResult4 = session.execute(ss4);
        for (Row row : selectResult4) {
            //Se muestra por consola información de cada registro
            System.out.format("cuenta: %d\n", row.getLong("cuenta"));
        }


        //CASO 6: Se hace una agrupación y se cuantan los valores agrupados
        Select select5 = selectFrom("lectura_hash")
                .selector(Selector.column("hash"))
                .selector(Selector.countAll().as("cuenta"))
                .groupBy("hash");

        SimpleStatement ss5 = select5.build();


        //Se visualizan los resultados
        ResultSet selectResult5 = session.execute(ss5);
        for (Row row : selectResult5) {
            //Se muestra por consola información de cada registro
            System.out.format("hash: %s, cuenta: %d\n", row.getString("hash"), row.getLong("cuenta"));
        }


//        Select select6 = selectFrom("lectura_hash")
//                .divide(
//                        Selector.multiply(Selector.column("metodo"), literal(10_000)),
//                        Selector.multiply(Selector.column("origen"), Selector.column("valor")))
//                .as("bmi");
//
//        SimpleStatement ss6 = select6.build();

        //se cierra la sesion
        session.close();

        System.out.println("FIN\n\n");

    }
}
