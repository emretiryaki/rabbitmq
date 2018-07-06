# rabbitmq Wrapper
RabbitMq Wrapper is the a client API for RabbitMQ. 

* A  wrapper over [amqp](https://github.com/streadway/amqp) exchanges and queues.
* In memory retries for consuming messages when an error occured
* CorrelationId and MessageId structure
* Retry policy (will be added)
* Some extra features while publishing message  (will be added) 

To connect to a RabbitMQ broker...

    	var messageBus=rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/")

To connect to a RabbitMQ broker with retry policy 
 * Consumer retries two times if an error occured

      	var messageBus=rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/",
                                                  rabbit.RetryCount(2))
                                                  
 To send a message 
        
        messageBus.Publish(PersonV2{Name: "Adam", Surname: "Smith"}, rabbit.WithCorrelationId(uuid.New().String()))
        
 To consume a message
 
         onConsumed := func(message rabbit.Message) error {
              var consumeMessage PersonV2
              json.Unmarshal(message.Payload, &consumeMessage)
              fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:",consumeMessage)
              ...Some code....
              return nil
            }
            messageBus.Consume("In.Person",  PersonV2{}, onConsumed)
