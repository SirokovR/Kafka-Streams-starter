package io.confluent.developer.starter;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
@EnableKafkaStreams
public class StreamsApplication {


    @Bean
    NewTopic quotes(){
        return new NewTopic("quotes",1,(short) 1);
    }

    @Bean
    NewTopic counts(){
        return new NewTopic("counts",1,(short) 1);
    }

    public static void main(String[] args) {
        SpringApplication.run(StreamsApplication.class, args);
    }

}

@Component
class Processor{
    @Autowired
    public void process(final StreamsBuilder builder){
        final Serde<String> stringSerde = Serdes.String();
        builder.stream("quotes", Consumed.with(stringSerde,stringSerde))
                .flatMapValues( value -> {
                    final String[] split = value.toLowerCase().split(" \\w+");
                    return Arrays.asList(split);
                })
                .groupBy(((key, value) -> value), Grouped.with(stringSerde, stringSerde))
                .count()
                .toStream()
                .to("counts");

    }
}
