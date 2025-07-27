package rabbit

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Rabbit struct{
	rClient 	*amqp.Connection
	ExchangerName string
	QueueNames 		[]string
}

func CreateRabbit(rConn *amqp.Connection,exchanger string,queueNames ...string) Rabbit{
	rabbit := Rabbit{rClient: rConn}
	ch,err := rConn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	if err := rabbit.CreateEchanger(ch,exchanger);err !=nil {
		panic(fmt.Errorf("Error when create Exchanger %w",err))
	}
	if err := rabbit.CreateQueues(ch,queueNames...);err != nil{
		panic(fmt.Errorf("Error when create Queue %w",err))
	}
	if err := rabbit.BindQueues(ch);err != nil{
		panic(fmt.Errorf("Error when bind Queue %w",err))
	}

	return rabbit
}

func(r *Rabbit) CreateEchanger(ch *amqp.Channel,exchanger string) (error){
	if err := ch.ExchangeDeclare(
						exchanger,
						"direct",
						true,
						false,        
						false,        
						false,        
						nil,
	); err != nil {
		return err
	}

	r.ExchangerName = exchanger 
	return nil
}
func(r *Rabbit) CreateQueues(ch *amqp.Channel,queueNames ...string) (error){
	for _,name := range queueNames{
		if _,err := ch.QueueDeclare(
					name,
					true,
					false,
					false,
					false,
					nil,
		); err != nil {
			return err
		}
	}
	r.QueueNames = append(queueNames, queueNames...)
	return nil
}

func(r *Rabbit) BindQueues(ch *amqp.Channel) (error){
	for _,queue := range r.QueueNames{
		if err := ch.QueueBind(queue,queue,r.ExchangerName,false,nil,);err != nil {
			return err
		}
	}
	return nil
}

func(r *Rabbit) PublishMessage(ctx context.Context,msg []byte,key string,ch *amqp.Channel) (error){
	if err := ch.PublishWithContext(ctx,
		r.ExchangerName,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType:     "text/plain",
			Body:            msg,
			DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
		},
	);err != nil{
		return err
	}

	return nil
}

func(r *Rabbit) PublishMessageWithTx(ctx context.Context,msg []byte,key string,resCh <-chan bool) (error){
	ch,err := r.rClient.Channel()
	if err != nil {
		return err
	}
	if err = ch.Tx();err != nil {
		ch.Close()
		return err
	}

	r.PublishMessage(ctx,msg,key,ch)
	select{
		case <-ctx.Done():
			if err = ch.TxRollback();err != nil{
				return err
			}
		case res := <-resCh:
			if res {
				if err = ch.TxCommit();err != nil{
					return err
				}
			}else{
				if err = ch.TxRollback();err != nil{
					return err
				}
			}
	}
	return nil
}

func(r *Rabbit) ConsumeMessages(ctx context.Context,key string) (chan string,error) {
	resCh := make(chan string)
	ch,err := r.rClient.Channel()

	if err != nil {
		slog.Error(fmt.Sprint(err))
		return nil,err
	}

	list,err := ch.ConsumeWithContext(ctx,
		key,
		r.ExchangerName,
		false,
		false,      
		false,      
		false,      
		nil,
	)		

	if err != nil {
		slog.Error(fmt.Sprint(err))
		return nil,fmt.Errorf("RabbitMQ error %w",err)
	}

	go func () {
		var first []byte

		for {
			select{
				case msg := <-list:
					slog.Info("Take out exchange id from queue")
					if bytes.Equal(first,msg.Body){
						slog.Info("Close channel")
						ch.Close()
						close(resCh)
						return 
					}
					if first == nil {
						first = msg.Body 			
					}
					resCh <- string(msg.Body)
					msg.Nack(false,true) 
			
				case <-ctx.Done():
						slog.Info("Close channel with timeout")
						close(resCh)
						ch.Close()
						return 
			}
		}
	}()

	return resCh,nil
}

func(r *Rabbit) DeleteMessage(ctx context.Context,id []byte,key string,ch *amqp.Channel) (error) {
	list,err := ch.ConsumeWithContext(ctx,
		key,
		r.ExchangerName,
		false,
		false,      
		false,      
		false,      
		nil,
	)		
	if err != nil {
		return fmt.Errorf("RabbitMQ error %w",err)
	}
	go func() {
		first := []byte{}
		for {
			select{
				case msg := <-list:
					if bytes.Equal(first,msg.Body){
						ch.Close()
						return 
					}
					if first == nil{
						first = msg.Body		
					}
					if bytes.Equal(msg.Body,id){
						msg.Ack(true)	
					}		
					msg.Nack(false,true)
				case <-ctx.Done():
					ch.Close()
					return 
			}
		}
	}()
	return nil
}


func(r *Rabbit) DeleteMessageWithTx(ctx context.Context,id []byte,key string,resCh <-chan bool) (error) {
	ch,err := r.rClient.Channel()
	if err != nil {
		return err
	}
	if err = ch.Tx();err != nil {
		ch.Close()
		return err
	}

	r.DeleteMessage(ctx,id,key,ch)
	select{
		case <-ctx.Done():
			if err = ch.TxRollback();err != nil{
				return err
			}
		case res := <-resCh:
			if res {
				if err = ch.TxCommit();err != nil{
					return err
				}
			}else{
				if err = ch.TxRollback();err != nil{
					return err
				}
			}
	}
	return nil
}
