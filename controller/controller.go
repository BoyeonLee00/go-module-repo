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
	"time"

	"github.com/joho/godotenv"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Error struct {
	StatusCode  int    `json:"status_code"`
	Error       string `json:"error"`
	Description string `json:"description"`
}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s : %s\n", msg, err)
	}
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
