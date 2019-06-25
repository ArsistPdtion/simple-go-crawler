package connection_mongodb

import (
	"context"
	"github.com/ArsistPdtion/workbook/my_crawler/simple_e1/data_model"
	"github.com/labstack/gommon/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	)


func Connect(url string) *mongo.Client{
	//set client options
	clientOptions := options.Client().ApplyURI(url)
	//connect to mongodb
	client, err := mongo.Connect(context.TODO(),clientOptions)
	if err != nil{
		log.Fatal(err)
	}
	//check the connections
	err = client.Ping(context.TODO(),nil)
	if err!=nil{
		log.Fatal(err)
	}
	//collection := client.Database(database).Collection(coll)
	//return collection
	return client
}

func DisConnect(client *mongo.Client){
	err := client.Disconnect(context.TODO())
	if err!=nil{
		log.Fatal(err)
	}
}

func ConnCollection(client *mongo.Client, database, coll string)*mongo.Collection{
	collection := client.Database(database).Collection(coll)
	return collection
}

func InsertOneData(collection *mongo.Collection, data data_model.Movies)error{
	_, err := collection.InsertOne(context.TODO(),data)
	if err != nil{
		log.Fatal(err)
	}
	return nil
}

func InsertManyData(collection *mongo.Collection, datas []interface{})error{
	_, err := collection.InsertMany(context.TODO(),datas)
	if err != nil{
		log.Fatal(err)
	}
	return nil
}