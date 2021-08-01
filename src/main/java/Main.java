import com.binance.api.client.BinanceApiClientFactory;
import com.binance.api.client.BinanceApiRestClient;


import java.io.IOException;


public class Main {
    public static void main(String[] args) throws IOException {
        //Connecting to Binance API
        BinanceApiClientFactory factory = BinanceApiClientFactory.newInstance();
        BinanceApiRestClient client = factory.newRestClient(); //Connected to Binance API

        //Information_Receiving_Subsystem: Getting data from Binance API and send these date to KafKa using Kafka Producer
        Information_Receiving_Subsystem.main(client);

        //Rule_Evaluator_Subsystem: Fetching these data from Kafka using KafKa Consumer and save them in Mysql Database
        Rule_Evaluator_Subsystem.main();

        //And finally sending HTTP Request to API and getting data from Database and showing them to client
        //...

    }
}
