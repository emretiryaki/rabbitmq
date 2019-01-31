# rabbitmq Wrapper
RabbitMq Wrapper is the a client API for RabbitMQ. 

* A  wrapper over [amqp](https://github.com/streadway/amqp) exchanges and queues.
* In memory retries for consuming messages when an error occured
* CorrelationId and MessageId structure
* Exchange Types With Direct and Fanout and Topic
* Retry policy (immediately , interval)
* Multiple consumers In a single process
* Create goroutines and consume messages asynchronously 
* Some extra features while publishing message  (will be added) 

To connect to a RabbitMQ broker...

    	var rabbitClient=rabbit.NewRabbitMqClient("amqp://guest:guest@localhost:5672/")

To connect to a RabbitMQ broker with retry policy 
 * Consumer retries two times immediately if an error occured

      	var rabbitClient=rabbit.NewRabbitMqClient("amqp://guest:guest@localhost:5672/",
                                                  rabbit.RetryCount(2,time.Duration(0)))
  
 * Create goroutines and consume messages asynchronously using PrefetchCount Prefix. 
 Create as number of  PrefetchCount as goroutines .
 
       	var rabbitClient=rabbit.NewRabbitMqClient("amqp://guest:guest@localhost:5672/",
                       		rabbit.PrefetchCount(3))
                                                    
 To send a message 
        
        // Added
        
 To consume a message
 
        onConsumed := func(message rabbit.Message) error {
        
        		var consumeMessage PersonV1
        		var err= json.Unmarshal(message.Payload, &consumeMessage)
        		if err != nil {
        			return err
        		}
        		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:", consumeMessage)
        		return nil
        	}
        
  
        rabbitClient.AddConsumer("In.Person", "PersonV1","", onConsumed)
    
 To Consume multiple messages

    	onConsumed := func(message rabbit.Message) error {
    
    		var consumeMessage PersonV1
    		var err= json.Unmarshal(message.Payload, &consumeMessage)
    		if err != nil {
    			return err
    		}
    		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:", consumeMessage)
    		return nil
    	}
    
    	onConsumed2 := func(message rabbit.Message) error {
    
    		var consumeMessage PersonV4
    		var err= json.Unmarshal(message.Payload, &consumeMessage)
    		if err != nil {
    			return err
    		}
    		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:", consumeMessage)
    		return nil
    	}
    	rabbitClient.AddConsumer("In.Person3", "PersonV3","", onConsumed2)
    	rabbitClient.AddConsumer("In.Person", "PersonV1","", onConsumed)
    
    	rabbitClient.RunConsumers()