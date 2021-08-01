import com.binance.api.client.domain.market.Candlestick;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Rule_Evaluator_Subsystem {
    static double[] BTC_Mean = new double[30];
    static double[] ETH_Mean = new double[7];
    static String currenPrice;
    static int numHour = 0;
    static int numDay = 0;
    static double sum = 0.0;
    static double mean = 0.0;
    public static void main() throws IOException {
        String bootstrapServers = "127.0.0.1:9092";
        String BTC_Topic = "BTCTopic";
        String ETH_Topic = "ETHTopic";
        String groupId = "XYZ";

        //Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Create Database
        mySql.createDatabase();
        //Create Table
        mySql.createTable();

        //BTCTopic Evaluation Block
        {
            //Subscribe consumer to BTC topic:
            consumer.subscribe(Collections.singleton(BTC_Topic));
            //Poll for new data
            ConsumerRecords<String, String> BTC_records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : BTC_records) {
                calculateMeanPrice(jsonToList(record.value()), BTC_Topic);
            }
            BTC_Mean[numDay] = (mean / 24);
            if (ruleEvaluator(7, BTC_Topic) > ruleEvaluator(30, BTC_Topic)) {
                mySql.insertToTable("BTC/USDT Rule", "BTCUSDT", currenPrice);
            }

        }
        numDay = 0;
        numHour = 0;
        sum = 0.0;
        mean = 0.0;
        //ETHTopic Evaluation Block
        {
            //Subscribe consumer to ETH topic:
            consumer.subscribe(Collections.singleton(ETH_Topic));
            //Poll for new data
            ConsumerRecords<String, String> ETH_records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : ETH_records) {
                calculateMeanPrice(jsonToList(record.value()), ETH_Topic);
            }
            ETH_Mean[numDay] = (mean / 24);
            if (ruleEvaluator(3, "ETHTopic") < ruleEvaluator(7, ETH_Topic)) {
                mySql.insertToTable("ETH/BTC Rule", "ETHBTC", currenPrice);
            }
        }
    }

    public static void calculateMeanPrice(List<Candlestick> candlesticks, String topicName) {
        if (numHour == 0 && numDay == 0)
            currenPrice = topicName.equals("BTCTopic") ? candlesticks.get(candlesticks.size() - 1).getClose() :
                    candlesticks.get(candlesticks.size() - 1).getClose();
        numHour++;
        if (numHour > 24) {
            if (topicName.equals("BTCTopic")) {
                BTC_Mean[numDay] = (mean / 24);
            } else if (topicName.equals("ETHTopic")) {
                ETH_Mean[numDay] = (mean / 24);
            }
            numDay++;
            numHour = 1;
            mean = 0.0;
        }
        sum = 0.0;
        if (topicName.equals("BTCTopic")) {
            for (Candlestick candlestick : candlesticks) {
                sum += Double.parseDouble(candlestick.getClose());
            }
        } else if (topicName.equals("ETHTopic")) {
            for (Candlestick candlestick : candlesticks) {
                sum += Double.parseDouble(candlestick.getOpen());
            }
        }
        mean += sum / 60;
    }
    public static double ruleEvaluator(int head, String topicName) {
        double MEAN = 0.0;
        if (topicName.equals("BTCTopic")) {
            for (int i = 0; i < head; i++)
                MEAN += BTC_Mean[i];
        } else if (topicName.equals("ETHTopic")) {
            for (int i = 0; i < head; i++) {
                MEAN += ETH_Mean[i];
            }
        }
        return MEAN/head;
    }
    public static List<Candlestick> jsonToList(String str) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        TypeReference<List<Candlestick>> mapType = new TypeReference<>() {};
        List<Candlestick> candlesticks = objectMapper.readValue(str, mapType);
        return candlesticks;
    }
}