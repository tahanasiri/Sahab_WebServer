# Sahab_WebServer
A web server project for Sahab Company entrance test

THIS PROJECT CONSISTS OF THREE PARTS:


Part 1: Information_Receiving_Subsystem.java

Gets the market information from the Binance API and dumps this information into the KafKa queue.



Part 2: Rule_Evaluator_Subsystem.java

Reads this information from KafKa queue and stores it in the Mysql Database.

mySql.java: Creating Database Operation, Creating Table Operation and Inserting into the table Operation.
        
        
        
Part 3: Backend.java

Retrieves data from the Database by an API and shows it to the user.
