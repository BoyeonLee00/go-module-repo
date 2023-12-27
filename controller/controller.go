package controller

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/joho/godotenv"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Tag
// v0.0.1 - FailOnError 삭제 후 PrintError 추가됨
// v0.0.2 - kafka 함수 추가됨

type Error struct {
	StatusCode  int    `json:"status_code"`
	Error       string `json:"error"`
	Description string `json:"description"`
}

func PrintError(err error, msg string) {
	_, file, line, _ := runtime.Caller(1)
	log.Printf("%s : %v at %s:%d\n", msg, err, file, line)
}

func SendMessage(ch *amqp.Channel, tag string, message map[string]interface{}, rk string) error {
	headers := make(amqp.Table)
	headers["tag"] = tag

	body, err := json.Marshal(message)
	if err != nil {
		return errors.Wrap(err, "[SendMessage] Fail to convert a message to JSON")
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         body,
		Headers:      headers,
	}

	err = ch.Publish(
		"ex-indy", // exchange
		rk,        // routing key
		false,     // mandatory
		false,     // immediate
		msg)
	if err != nil {
		return errors.Wrap(err, "[SendMessage] Fail to send a message to the queue")
	}

	return nil
}

func SendErrorMessage(ch *amqp.Channel, tag int, message Error, rk string) error {
	headers := make(amqp.Table)
	headers["tag"] = tag

	body, err := json.Marshal(message)
	if err != nil {
		return errors.Wrap(err, "[SendErrorMessage] Fail to convert a message to JSON")
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         body,
		Headers:      headers,
	}

	err = ch.Publish(
		"ex-indy", // exchange
		rk,        // routing key
		false,     // mandatory
		false,     // immediate
		msg)
	if err != nil {
		return errors.Wrap(err, "[SendErrorMessage] Fail to send a message to the queue")
	}

	return nil
}

func GetDB() (*sql.DB, error) {
	err := godotenv.Load(".env")
	if err != nil {
		return nil, errors.Wrap(err, "[GetDB] Fail to load an env")
	}

	dbHost := os.Getenv("DB_HOST")
	database := os.Getenv("DATABASE")
	pgUser := os.Getenv("POSTGRES_USER")
	pgPassword := os.Getenv("POSTGRES_PASSWORD")

	dbinfo := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", dbHost, pgUser, pgPassword, database)

	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		return nil, errors.Wrap(err, "[GetDB] Fail to open postgresql")
	}

	return db, nil
}

func GetRabbitmq() (*amqp.Connection, *amqp.Channel, error) {
	rabbitmqIP := os.Getenv("RABBITMQ_IP")
	rabbitmqPort := os.Getenv("RABBITMQ_PORT")
	rabbitmqUser := os.Getenv("RABBITMQ_USER")
	rabbitmqPassword := os.Getenv("RABBITMQ_PASSWORD")

	amqpURL := fmt.Sprintf("amqp://%s:%s@%s:%s/", rabbitmqUser, rabbitmqPassword, rabbitmqIP, rabbitmqPort)

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, nil, errors.Wrap(err, "[GetRabbitmq] Fail to connect Rabbitmq")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, errors.Wrap(err, "[GetRabbitmq] Fail to create a channel")
	}

	err = ch.ExchangeDeclare(
		"ex-indy", // name
		"direct",  // kind
		true,      // durable
		false,     // autoDelete
		false,     // internal
		false,     // noWait
		nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "[GetRabbitmq] Fail to declare an exchange")
	}

	return conn, ch, nil
}

func SendHTTPRequest(method string, url string, data map[string]interface{}) (map[string]interface{}, error) {
	pbytes, _ := json.Marshal(data)
	buff := bytes.NewBuffer(pbytes)
	req, err := http.NewRequest(method, url, buff)
	if err != nil {
		return nil, errors.Wrap(err, "[SendHTTPRequest] request error")
	}

	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		return nil, errors.Wrap(err, "[SendHTTPRequest] api error")
	}

	var result map[string]interface{}

	respbytes, _ := ioutil.ReadAll(resp.Body)
	json_err := json.Unmarshal(respbytes, &result)
	if err != nil {
		return nil, errors.Wrap(json_err, "[SendHTTPRequest] Fail to unmarshal bytes to map")
	}

	return result, nil
}

func HolderSendHTTPRequest(walletToken string, method string, url string, data map[string]interface{}) (map[string]interface{}, error) {
	var req *http.Request
	var err error

	if data == nil {
		req, err = http.NewRequest(method, url, nil)
	} else {
		pbytes, _ := json.Marshal(data)
		buff := bytes.NewBuffer(pbytes)
		req, err = http.NewRequest(method, url, buff)
	}
	if err != nil {
		return nil, errors.Wrap(err, "[SendHTTPRequest] request error")
	}

	token := "Bearer " + walletToken

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", token)

	client := &http.Client{}
	resp, err := client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		return nil, errors.Wrap(err, "[SendHTTPRequest] api error")
	}

	var result map[string]interface{}

	respbytes, _ := ioutil.ReadAll(resp.Body)
	json_err := json.Unmarshal(respbytes, &result)
	if err != nil {
		return nil, errors.Wrap(json_err, "[SendHTTPRequest] Fail to unmarshal bytes to map")
	}

	return result, nil
}

func KafkaProducer(kafkaServer string) (*kafka.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaServer})
	if err != nil {
		return nil, errors.Wrap(err, "[KafkaProducer] NewProducer error")
	}

	return p, nil
}

func Produce(message map[string]interface{}, topic string, key string) error {
	kafkaServer := os.Getenv("KAFKA_SERVER")
	p, err := KafkaProducer(kafkaServer)
	if err != nil {
		return errors.Wrap(err, "[Produce] kafkaConfig error")
	}
	defer p.Close()

	bytemessage, err := json.Marshal(message)
	if err != nil {
		return errors.Wrap(err, "[Produce] Fail to marshal JSON")
	}

	errCh := make(chan error)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					errCh <- errors.Wrap(ev.TopicPartition.Error, "[Produce] Fail to deliver message")
					return
				}
			}
		}
	}()

	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          bytemessage,
	}

	err = p.Produce(kafkaMessage, nil)
	if err != nil {
		return errors.Wrap(err, "[Produce] Fail to produce message")
	}

	p.Flush(5000)

	select {
	case produceErr := <-errCh:
		return produceErr
	default:
		return nil
	}
}

func ProduceError(errorMessage Error, topic string, key string) error {
	kafkaServer := os.Getenv("KAFKA_SERVER")
	p, err := KafkaProducer(kafkaServer)
	if err != nil {
		return errors.Wrap(err, "[ProduceError] kafkaConfig error")
	}
	defer p.Close()

	bytemessage, err := json.Marshal(errorMessage)
	if err != nil {
		return errors.Wrap(err, "[ProduceError] Fail to marshal JSON")
	}

	errCh := make(chan error)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					errCh <- errors.Wrap(ev.TopicPartition.Error, "[ProduceError] Fail to deliver message")
					return
				}
			}
		}
	}()

	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          bytemessage,
	}

	err = p.Produce(kafkaMessage, nil)
	if err != nil {
		return errors.Wrap(err, "[ProduceError] Fail to produce message")
	}

	p.Flush(5000)

	select {
	case produceErr := <-errCh:
		return produceErr
	default:
		return nil
	}
}

func KafkaConsumer(kafkaServer string, groupID string) (*kafka.Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServer,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return nil, errors.Wrap(err, "[KafkaConsumer] NewConsumer error")
	}

	return c, nil
}
