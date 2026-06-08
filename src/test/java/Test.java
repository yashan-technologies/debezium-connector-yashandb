import org.apache.kafka.connect.cli.ConnectStandalone;

public class Test {
    public static void main(String[] args) {

        ConnectStandalone.main(new String[]{
                "src/test/resources/connect-standalone.properties",
                "src/test/resources/yashandb-connector-source.properties"
        });
    }
}
