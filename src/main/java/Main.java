import com.binance.api.client.BinanceApiClientFactory;
import com.binance.api.client.BinanceApiRestClient;
import com.binance.api.client.domain.market.Candlestick;
import com.binance.api.client.domain.market.CandlestickInterval;

import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args){
        Scanner input = new Scanner(System.in);

        //Connecting to Binance API
        BinanceApiClientFactory factory = BinanceApiClientFactory.newInstance();
        BinanceApiRestClient client = factory.newRestClient(); //Connected to Binance API

        long endTime = System.currentTimeMillis();
        long startTime =  endTime - 3600000;

        //Taking input the market symbol
        String symbol = input.next();

        //e.g: Calculate mean for past 24 Hours:
        //System.out.println(calMeanForONE_D(client,symbol,startTime,endTime));

    }

    // Calculate mean for the past One Hour
    public static double calMeanForONE_H(List<Candlestick> candlesticks){
        double sum = 0.0;
        for (Candlestick candlestick : candlesticks) {
            sum += Double.parseDouble(candlestick.getOpen());
        }

        return sum/candlesticks.size();
    }
    //Calculate mean using calMeanForONE_H function for the past 24 Hours
    public static double calMeanForONE_D(BinanceApiRestClient client, String symbol, long startTime, long endTime){
        List<Candlestick> candlesticks;
        double sumOfMean = 0.0;
        for (int i = 1; i <=24 ; i++) {
            candlesticks = client.getCandlestickBars(symbol,CandlestickInterval.ONE_MINUTE,500,startTime,endTime);
            sumOfMean +=calMeanForONE_H(candlesticks);
            endTime = startTime;
            startTime = endTime - 3600000;
        }

        return sumOfMean/24;
    }
    //Calculate mean using calMeanForONE_D function for the past N Days
    public static double calMeanForN_Days(BinanceApiRestClient client, String symbol, long startTime, long endTime, int numberOfDays){
        double sumOfMean = 0.0;
        for (int i = 1; i <=numberOfDays ; i++) {
            sumOfMean +=calMeanForONE_D(client,symbol,startTime,endTime);
            endTime = startTime;
            startTime = endTime - 86400000;
        }

        return sumOfMean/numberOfDays;
    }

}

