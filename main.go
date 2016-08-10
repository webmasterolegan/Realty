package main

import (
	"fmt"
	"github.com/opesun/goquery"
	"regexp"
	//"sort"
	"strconv"
	//"strings"
)

const (
	MongoDBHosts = "ds035428.mongolab.com:35428"
	AuthDatabase = "goinggo"
	AuthUserName = "guest"
	AuthPassword = "welcome"
	TestDatabase = "DataBox"
)

var (
	Start_Page  int         = 1
	PrezentUIDs chan string = make(chan string)

	// URLs
	Farpost_URL string = "http://www.farpost.ru/khabarovsk/realty/"

	Prezent_URL      string = "http://present-dv.ru/present/notice/index/rubric/nedvijimost-prodam/pageSize/200/"
	Prezent_Card_URL string = "http://present-dv.ru/present/notice/"
)

// Подсчет количества страниц
func PrezentPages() int {
	Pages, err := goquery.ParseUrl(Prezent_URL)
	if err != nil {
		panic(err)
	}
	// Список страниц со списками обьявлений
	List := Pages.Find(".pager a").Attrs("href")
	n := len(List) - 2
	re := regexp.MustCompile("[0-9]+$")
	num, _ := strconv.Atoi(re.FindString(List[n]))

	return num
}

// Сбор списков обьявлений
func UidCollector() {
	pages := PrezentPages() + 1

	//fmt.Println(pages)

	for i := 1; i < pages; i++ {
		url := Prezent_URL + strconv.Itoa(i)
		fmt.Println("Добавлен URL:", url)

		UidFinder(PrezentUIDs, url)
	}
}

func UidFinder(PrezentUIDs chan string, url string) {
	//fmt.Println(url)

	Page, err := goquery.ParseUrl(url)
	if err != nil {
		panic(err)
	}

	// Сбор всех ссылок
	Links := Page.Find("a").Attrs("href")

	re := regexp.MustCompile("view/[0-9]+$")

	for i := 0; i < len(Links); i++ {
		if re.FindString(Links[i]) != "" {
			fmt.Println(re.FindString(Links[i]))
		}
	}
}

func main() {
	UidCollector()
}
