import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

import com.github.javafaker.Faker

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import java.util.Date

private fun createProducer(brokers: String): Producer<String, String> {
    val props = Properties()
    props["bootstrap.servers"] = brokers
    props["key.serializer"] = StringSerializer::class.java.canonicalName
    props["value.serializer"] = StringSerializer::class.java.canonicalName
    return KafkaProducer(props)
}

data class Person(
    val firstName: String,
    val lastName: String,
    val birthDate: Date
)

class KafkaProducer {
    companion object {
        @JvmStatic
        fun main(args: Array<String>){
            val faker = Faker()
            val jsonMapper = ObjectMapper().apply {
                registerKotlinModule()
                disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                dateFormat = StdDateFormat()
            }
            while(true) {
                val fakePerson = Person(
                    firstName = faker.name().firstName(),
                    lastName = faker.name().lastName(),
                    birthDate = faker.date().birthday()
                )
                val fakePersonJson = jsonMapper.writeValueAsString(fakePerson)
                val producer = createProducer("localhost:9092")

                producer.send(ProducerRecord("my-topic", fakePersonJson))
                Thread.sleep(3600)
            }
        }
    }
}