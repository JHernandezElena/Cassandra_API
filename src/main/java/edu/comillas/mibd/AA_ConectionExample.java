package edu.comillas.mibd;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;

public class AA_ConectionExample {
    public static void main(String[] args){

        //Si no se especifica ningún nodo se conecta al puerto 9042 de la maquina en la que se ejecuta
        try (CqlSession session = CqlSession.builder().build()) {
            ResultSet rs = session.execute("SELECT release_version FROM system.local");
            Row row = rs.one();
            System.out.println(row.getString("release_version"));

            Metadata meta = session.getMetadata(); //PARA OBTENER EL METADATO (INFO DEL CLUSTER)
            Map<UUID, Node> nodes=  meta.getNodes(); //mapa de nodos

            for(Map.Entry<UUID, Node> it : nodes.entrySet()){ //PINTA INFO DEL CLUSTER
                System.out.printf("UUID: %s; listenAddress: %s;\n",
                        it.getKey().toString(), it.getValue().getListenAddress().toString());
            }
        }


        // Para definir un nodo al que conectarse
        InetSocketAddress isa = InetSocketAddress.createUnresolved("127.0.0.1", 9042);

        //para conocer el datacenter ejecutar en cql "select data_center from system.local";
        CqlSession session = CqlSession.builder()
                .withLocalDatacenter("datacenter1") //EL DATA CENTER AL QUE ME QUIERO CONECTAR
                .addContactPoint(isa)
                .build();
        ResultSet rs = session.execute("SELECT release_version FROM system.local");
        Row row = rs.one();
        System.out.println(row.getString("release_version")); //LE PIDO LA VERSION DE CASANDRA
        //Se cierra la sesión
        session.close();


        //se puede fijar el key Space por defecto en la sesion
        CqlSession session2 = CqlSession.builder()
                .withKeyspace(CqlIdentifier.fromCql("redfija"))
                .build();
        //Se cierra la sesión
        session2.close();


        System.out.println("FIN\n\n");

    }
}