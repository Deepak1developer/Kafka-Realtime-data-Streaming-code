##**Demo Application Representing how to set up Kafka in python.**

Follow the following steps to make it work in your local system.</br>
step1-->  Install kafka into your system and extract in any location where you like to.</br>
step2-->  After installing run the following command in 2 different terminals one for kafka and other for zookeeper.</br>
- Start the zookeeper.<br>
bin/zookeeper-server-start.sh config/zookeeper.properties
<br>

- To start the kafka server.<br>
bin/kafka-server-start.sh config/server.properties
<br>
  
step3--> After the above step Run the main.py and you will see that the producer will the dump the data into the kafka 
and consumer function will get the data from that topic </br>

