import com.binance.api.client.BinanceApiClientFactory;
import com.binance.api.client.BinanceApiRestClient;
import com.binance.api.client.domain.market.Candlestick;
import com.binance.api.client.domain.market.CandlestickInterval;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        //Connecting to Binance API
        BinanceApiClientFactory factory = BinanceApiClientFactory.newInstance();
        BinanceApiRestClient client = factory.newRestClient(); //Connected to Binance API

        long endTime = System.currentTimeMillis();
        long startTime = endTime - 3600000;

        //Logger logger = LoggerFactory.getLogger(Main.class);
        String bootstrapServers = "127.0.0.1:9092";

        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Get BTC/USDT Market from API and send it to KafKa
        String BTC_Symbol = "BTCUSDT";
        String BTC_TopicName = "BTCTopic";
        sendThePastN_DaysDataToKafka(client, producer, BTC_Symbol, BTC_TopicName, startTime, endTime,30);

        //Get ETH/BTC Market from API and send it to KafKa
        String ETH_Symbol = "ETHBTC";
        String ETH_TopicName = "ETHTopic";
        sendThePastN_DaysDataToKafka(client, producer, ETH_Symbol, ETH_TopicName, startTime, endTime,7);

        //Flush data
        producer.flush();

        //Flush and Close producer
        producer.close();
    }

    //Get the past ONE Day market info from API and send it to the KafKa
    public static void sendThePastONE_DayDataToKafka(BinanceApiRestClient client, KafkaProducer<String, String> producer, String symbol, String topic, long startTime, long endTime){
        List<Candlestick> candlesticks;
        for (int i = 1; i <=24 ; i++) {
            //Get from API
            candlesticks = client.getCandlestickBars(symbol,CandlestickInterval.ONE_MINUTE,500,startTime,endTime);

            //Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, listToString(candlesticks));

            //Send data - asynchronous
            producer.send(record);

            endTime = startTime;
            startTime = endTime - 3600000;
        }
    }
    //Get the past N [Maybe N=7 or N=30] Days market info from API and send it to the KafKa
    public static void sendThePastN_DaysDataToKafka(BinanceApiRestClient client, KafkaProducer<String, String> producer, String symbol,String topic, long startTime, long endTime, int numberOfDays){
        for (int i = 1; i <=numberOfDays ; i++) {
            sendThePastONE_DayDataToKafka(client, producer, symbol, topic, startTime, endTime);
            endTime = startTime;
            startTime = endTime - 86400000;
        }
    }

    public static String listToString(List<Candlestick> candlesticks){
        StringBuilder stringBuilder = new StringBuilder();
        for (Candlestick candlestick: candlesticks) {
            stringBuilder.append(candlestick);
            stringBuilder.append(",");
        }
        stringBuilder.setLength(stringBuilder.length()-1);
        return stringBuilder.toString();
    }
}
