package redis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/DEEBBLUE/ExProtos/api/Types"
	red "github.com/redis/go-redis/v9"

	Models "github.com/DEEBBLUE/Models/Models"
)

type Redis struct{
	redClient *red.Client
}

func Init(opt *red.Options) Redis {
	redClinet := red.NewClient(opt)

	return Redis{
		redClient: redClinet,
	}
}

func(redis *Redis) SetExchange(ctx context.Context,key string,exchange *Types.Exchange) (error) {
	var ex Models.Exchange

	ex.CreateFromGRPC(exchange)
	data,err := ex.CreateJson()
	fmt.Println(key)
		
	if err != nil {
		return err
	}

	err = redis.redClient.Set(ctx,key,data,0).Err()
	if err != nil {
		return err
	}
	return nil	
}

func(redis *Redis) GetExchange(ctx context.Context,key string) (Models.Exchange,error){
	var res Models.Exchange
	fmt.Println(key)

	data := redis.redClient.Get(ctx,key)
	
	if err := data.Err();err != nil {
		slog.Error(fmt.Sprint("Error in getting data: ",err))
		return res,err
	}

	bytesData,err := data.Bytes()
	if err != nil{
		slog.Error(fmt.Sprint("Error in convert data to bytes",err))
		return res,err
	}

	

	if err := res.CreateFromJson(bytesData);err != nil{
		slog.Error(fmt.Sprint("Error in convert data",err))
		return res,err
	}

	return res,nil
}
