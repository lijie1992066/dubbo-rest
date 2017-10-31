package cn.lijie.kafka;


public class MyProducer {

    public static void main(String[] args) throws Exception {
        produce("lijie","hahahaha1");
        produce("lijie","hahahaha2");
        produce("lijie","hahahaha3");
        Thread.sleep(1000);
    }

    public static void produce(String topic, String message) throws Exception {
        KafkaProduceClient.getInstance().sendKafkaMessage(topic, message);
    }

}
