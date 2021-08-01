import com.binance.api.client.BinanceApiRestClient;
import com.binance.api.client.domain.market.Candlestick;
import com.binance.api.client.domain.market.CandlestickInterval;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class Information_Receiving_Subsystem {
    public static void main(BinanceApiRestClient client) throws JsonProcessingException {
        long endTime = System.currentTimeMillis();
        long startTime = endTime - 3600000;
        String bootstrapServers = "127.0.0.1:9092";

        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Get BTC/USDT Market from API and send it to KafKa
        sendThePastN_DaysDataToKafka(client, producer, "BTCUSDT", "BTCTopic", startTime, endTime,30);

        //Get ETH/BTC Market from API and send it to KafKa
        sendThePastN_DaysDataToKafka(client, producer, "ETHBTC", "ETHTopic", startTime, endTime,7);

        //Flush data
        producer.flush();
        //Flush and Close producer
        producer.close();
    }

    //Get the past N [Maybe N=7 or N=30] Days market info from API and send it to the KafKa
    public static void sendThePastN_DaysDataToKafka(BinanceApiRestClient client, KafkaProducer<String, String> producer, String symbol,String topic, long startTime, long endTime, int numberOfDays) throws JsonProcessingException {
        for (int i = 1; i <=numberOfDays ; i++) {
            sendThePastONE_DayDataToKafka(client, producer, symbol, topic, startTime, endTime);
            //Update startTime and endTime to One hour ago
            endTime = startTime;
            startTime = endTime - 86400000;
        }
    }
    //Get the past ONE Day market info from API and send it to the KafKa
    public static void sendThePastONE_DayDataToKafka(BinanceApiRestClient client, KafkaProducer<String, String> producer, String symbol, String topic, long startTime, long endTime) throws JsonProcessingException {
        List<Candlestick> candlesticks;
        for (int i = 1; i <=24 ; i++) {
            //Get from API
            candlesticks = client.getCandlestickBars(symbol, CandlestickInterval.ONE_MINUTE,500,startTime,endTime);
            //Create a producer record and Send data - asynchronous
            producer.send(new ProducerRecord<>(topic, listToJson(candlesticks)));
            //Update startTime and endTime to One hour ago
            endTime = startTime;
            startTime = endTime - 3600000;
        }
    }

    public static String listToJson(List<Candlestick> candlesticks) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        String arrayToJson = objectMapper.writeValueAsString(candlesticks);
        return arrayToJson;
    }
}