# rabbitmq Wrapper
RabbitMq Wrapper is the a client API for RabbitMQ. 

* A  wrapper over [amqp](https://github.com/streadway/amqp) exchanges and queues.
* In memory retries for consuming messages when an error occured
* CorrelationId and MessageId structure
* Retry policy (immediately , interval)
* Create goroutines and consume messages asynchronously 
* Some extra features while publishing message  (will be added) 

To connect to a RabbitMQ broker...

    	var messageBus=rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/")

To connect to a RabbitMQ broker with retry policy 
 * Consumer retries two times immediately if an error occured

      	var messageBus=rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/",
                                                  rabbit.RetryCount(2,time.Duration(0)))
  
 * Create goroutines and consume messages asynchronously using PrefetchCount Prefix. 
 Create as number of  PrefetchCount as goroutines .
 
       	var messageBus=rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/",
                       		rabbit.PrefetchCount(3))
                                                    
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
