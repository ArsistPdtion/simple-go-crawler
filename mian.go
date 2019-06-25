package main

import (
	"fmt"
	"github.com/ArsistPdtion/workbook/my_crawler/simple_e1/connection_mongodb"
	"github.com/ArsistPdtion/workbook/my_crawler/simple_e1/connection_mysql"
	"github.com/ArsistPdtion/workbook/my_crawler/simple_e1/data_model"
	"github.com/PuerkitoBio/goquery"
	"github.com/labstack/gommon/log"
	"io"
	"net/http"
	"os"
	"reflect"
	"time"
	//_ "github.com/go-sql-driver/mysql"
)

const MONGO_URL = "mongodb://long:1234qwer@mongodb-2314-0.cloudclusters.net:10007/crawler?authSource=admin"
const MONGO_DATABASE = "crawler"
const MONGO_COLL = "movies"

func main() {
	start := time.Now()
	client := &http.Client{}
	var url = "https://movie.douban.com/"
	request, err := http.NewRequest("GET", url, nil)
	request.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36")
	response, err := client.Do(request)
	if err != nil {
		log.Printf("get error, reason is:%s", err)
		os.Exit(1)
	}
	fmt.Println("response is:", response.Status, response.StatusCode)
	fmt.Println("response type is:", reflect.TypeOf(response))
	defer response.Body.Close()
	//body,err := ioutil.ReadAll(response.Body)
	//if err!= nil{
	//	log.Printf("red response body error, reason is:%s",err)
	//}
	//fmt.Printf("body is:%s",body)
	doc, err := goquery.NewDocumentFromReader(response.Body)
	if err != nil {
		log.Fatal("New Document error:", err)
	}
	movieName := make([]string, 0)
	movieUrl := make([]string, 0)
	//get movies name
	doc.Find(".screening-bd .ui-slide-item .title").Each(func(i int, selection *goquery.Selection) {
		name := selection.Find("a").Text()
		fmt.Println("name is:", name)
		if (name != " ") {
			movieName = append(movieName, name)
		} else {
			fmt.Println("name is nil")
		}

	})
	//get movies img url
	doc.Find(".screening-bd .ui-slide-item .poster").Each(func(i int, selection *goquery.Selection) {
		imgSrc, _ := selection.Find("img").Attr("src")
		fmt.Println("a is:", imgSrc)
		movieUrl = append(movieUrl, imgSrc)
	})

	movies := make([]interface{}, 0)
	for i, _ := range listLen(movieName, movieUrl) {
		n := movieName[i] + ".jpg"
		u := movieUrl[i]
		movie := data_model.Movies{Name: n, ImgUrl: u}
		movies = append(movies, movie)
		//download img to local
		//downloadImg(u, n)
	}
	//store img link to mongodb
	//err = storeToMongo(movies)
	//store img link to mysql
	err = StoreToMysql(movies)
	if err != nil {
		log.Panic(err)
	}
	fmt.Println("time is:", time.Now().Sub(start))

}

func downloadImg(url, filename string) {
	fmt.Printf("url is:%s, filename is:%s\n", url, filename)
	res, err := http.Get(url)
	defer res.Body.Close()
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		log.Printf("download img file %s error: %s\n", filename, err)
	}
	log.Printf("start copy,file:%s,body:%s\n", file, res.Body)
	io.Copy(file, res.Body)
	log.Printf("end copy\n")
}

func listLen(a, b []string) []string {
	if len(a) >= len(b) {
		return b
	} else {
		return a
	}
}

func storeToMongo(datas []interface{}) error {
	client := connection_mongodb.Connect(MONGO_URL)
	defer connection_mongodb.DisConnect(client)
	collection := connection_mongodb.ConnCollection(client, MONGO_DATABASE, MONGO_COLL)
	err := connection_mongodb.InsertManyData(collection, datas)
	if err != nil {
		log.Panic(err)
	}
	return nil
}

func StoreToMysql(datas []interface{}) error {
	db := connection_mysql.ConnectMysql()
	defer connection_mysql.CloseMysql(db)
	err := connection_mysql.MysqlInsertManyData(db, datas)
	if err != nil {
		log.Fatal(err)
	}
	return nil
}
