package edu.comillas.mibd;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;


public class BA_StatementExample {
    public static void main(String[] args){

        CqlSession session = CqlSession.builder().build();
        //Se define una instrucción sql
        String query = "select hash, am, sum(valor) as suma from redfija.lectura_hash where hash='2#C##PJELR00000577#' group by hash,am";
        //Se crea un statement a partir del SQL
        SimpleStatement statement = SimpleStatement.newInstance(query).setIdempotent(false); //statement y statement2 son dos formas de hacer lo mismo
        SimpleStatement statement2 = SimpleStatement.builder(query).build().setIdempotent(false);
            //IMPORTANTE - EL SETIDEMPOTENT(true) - ASI SI SE EJECTUA MUCHAS VECES NO LE VA A PASAR NADA A LOS DATOS
                //en false se puede ejecutar muchas veces

        //Se ejecuta el statement
        ResultSet lecturaResult = session.execute(statement);
            //puedo hacer execute de un string o de un statement
            //a la statement le puedo pasar mas parametros (como el setIdempotent)


        //Se visualizan los resultados
        for (Row row : lecturaResult) {
            System.out.format("hash: %s, suma: %s, am:%s\n",
                    row.getString("hash"), row.getInt("suma"), row.getInt("am"));
        }

        //Se cierra la sesion
        session.close();

        System.out.println("FIN\n\n");
    }
}
