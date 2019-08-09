# rabbitmq Wrapper
RabbitMq Wrapper is the a client API for RabbitMQ. 

* A  wrapper over [amqp](https://github.com/streadway/amqp) exchanges and queues.
* In memory retries for consuming messages when an error occured
* CorrelationId and MessageId structure
* Exchange Types With Direct, Fanout, Topic, ConsistentHashing,  XDelayedMessage
* Retry policy (immediately , interval)
* Multiple consumers In a single process
* Create goroutines and consume messages asynchronously 
* Disable consume messages asynchronously if you want
* Retry to connect another node  When RabbitMq Node is Down or Broken Connection
* Add stack trace on the message header if the error occurred when the message is consumed
* Some extra features while publishing message  (will be added) 
Add support for [rabbitmq-delayed-message-exchange](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) plugin

To connect to a RabbitMQ broker...

    	var rabbitClient=rabbit.NewRabbitMqClient([]string{"127.0.0.1","127.0.0.2"},"guest","guest","/virtualhost")

To connect to a RabbitMQ broker with retry policy 
 * Consumer retries two times immediately if an error occured

      	var rabbitClient=rabbit.NewRabbitMqClient([]string{"127.0.0.1","127.0.0.2"},"guest","guest","/virtualhost",
                                                  rabbit.RetryCount(2,time.Duration(0)))
  
 * Create goroutines and consume messages asynchronously using PrefetchCount Prefix. 
 Create as number of  PrefetchCount as goroutines .
 
       	var rabbitClient=rabbit.NewRabbitMqClient([]string{"127.0.0.1","127.0.0.2"},"guest","guest","/virtualhost",
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
        
  
        rabbitClient.AddConsumer("In.Person").
        SubscriberExchange("RoutinKey.*",rabbit.Direct ,"Person").
        HandleConsumer(onConsumed)
    
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
    	rabbitClient.AddConsumer("In.Person3").
                SubscriberExchange("",rabbit.Fanout ,"ExchangeNamePerson").
                HandleConsumer(onConsumed)
                
        rabbitClient.AddConsumer("In.Person").
                 SubscriberExchange("Person.*",rabbit.Direct ,"PersonV1").
                 HandleConsumer(onConsumed2)
                 
    
    	rabbitClient.RunConsumers()

 To Consume multiple exchange
        
        rabbitClient.AddConsumer("In.Lines").
        		SubscriberExchange("1", rabbit.ConsistentHashing,"OrderLineAdded").
        		SubscriberExchange("1", rabbit.ConsistentHashing,OrderLineCancelled).
        		WithSingleGoroutine(true).
        		HandleConsumer(onConsumed2)

Use `rabbitmq_delayed_message_exchange` plugin

         args := make(amqp.Table)
        args["x-delayed-type"] = "direct"

         rabbitClient.AddConsumer("In.Person").
				SubscriberExchangeWithArguments(
					"Person.*",
					rabbitmq.XDelayedMessage,
					"delay-messages",
					args,
				).
				HandleConsumer(onConsumed2)
				