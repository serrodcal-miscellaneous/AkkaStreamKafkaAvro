package com.serrodcal.poc

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroDeserializerConfig, KafkaAvroSerializer}
import org.apache.avro.specific.{SpecificRecord, SpecificRecordBase}
import org.apache.kafka.common.serialization._

import scala.collection.JavaConverters._
import com.ing.eventbus.embedded.EmbeddedSingleNodeKafkaCluster
import org.apache.avro.{AvroRuntimeException, Schema}
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

object AvroStreamingApp extends App {

  val kafkaCluster : EmbeddedSingleNodeKafkaCluster = new EmbeddedSingleNodeKafkaCluster

  kafkaCluster.start
  kafkaCluster.createTopic("topic")

  implicit val system: ActorSystem = ActorSystem("cassandra-storage-stream")
  implicit val materializer: Materializer = ActorMaterializer()

  val kafkaAvroSerDeConfig: Map[String, Any] = Map(
    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> kafkaCluster.schemaRegistryUrl(),
    KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> true.toString
  )

  val producerSettings: ProducerSettings[String, SpecificRecord] = {
    val kafkaAvroSerializer = new KafkaAvroSerializer()
    kafkaAvroSerializer.configure(kafkaAvroSerDeConfig.asJava, false)
    val serializer = kafkaAvroSerializer.asInstanceOf[Serializer[SpecificRecord]]

    ProducerSettings(system, new StringSerializer, serializer)
      .withBootstrapServers(kafkaCluster.bootstrapServers())
  }

  val people = Seq(Person(1, "John", "Doe"), Person(2, "Alice", "Weber"), Person(3, "Bob", "Smith"))

  val producerCompletion =
    Source(people)
      .map(data => new ProducerRecord[String, SpecificRecord]("topic", data))
      .runWith(Producer.plainSink(producerSettings))

  val consumerSettings: ConsumerSettings[String, SpecificRecord] = {
    val kafkaAvroDeserializer = new KafkaAvroDeserializer()
    kafkaAvroDeserializer.configure(kafkaAvroSerDeConfig.asJava, false)
    val deserializer = kafkaAvroDeserializer.asInstanceOf[Deserializer[SpecificRecord]]

    ConsumerSettings(system, new StringDeserializer, deserializer)
      .withBootstrapServers(kafkaCluster.bootstrapServers())
      .withGroupId("group")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("topic"))
    .map(consumerRecord => {
      val specificRecord = consumerRecord.value()
      Client(specificRecord.get(0).toString, specificRecord.get(1) + " " + specificRecord.get(2))
    })
    .runWith(Sink.foreach(n => println(n)))
    .onComplete{
      case Success(_) => println("Done"); system.terminate()
      case Failure(err) => println(err.toString); system.terminate()
    }

}

case class Person(var id: Int, var name: String, var surname: String) extends SpecificRecordBase {

  def this() = this(0, null, null)

  override def get(i: Int): AnyRef = i match {
    case 0 => new Integer(id)
    case 1 => name
    case 2 => surname
    case index => throw new AvroRuntimeException(s"Unknown index: $index")
  }

  override def put(i: Int, v: scala.Any): Unit = i match {
    case 0 => id = asInteger(v)
    case 1 => name = asString(v)
    case 2 => surname = asString(v)
    case index => throw new AvroRuntimeException(s"Unknown index: $index")
  }

  private def asString(v: Any) =
    v match {
      case utf8: Utf8 => utf8.toString
      case _ => v.asInstanceOf[String]
    }

  private def asInteger(v: Any) =
    v match {
      case utf8: Utf8 => utf8.toString.toInt
      case _ =>  v.asInstanceOf[Int]
    }

  override def getSchema: Schema = Person.SCHEMA$
}

object Person {
  val SCHEMA$ : Schema =
    new org.apache.avro.Schema.Parser().parse("""
                                                |{"namespace": "com.serrodcal.poc",
                                                | "type": "record",
                                                | "name": "Person",
                                                | "fields": [
                                                |     {"name": "id", "type": "int"},
                                                |     {"name": "name", "type": "string"},
                                                |     {"name": "surname", "type": "string"}
                                                | ]
                                                |}
                                              """.stripMargin)
}

//case class Person(id: Int, name: String, surname: String)

case class Client(id: String, fullName: String)
