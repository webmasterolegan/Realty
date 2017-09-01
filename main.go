package main

import (
	"fmt"
	//stringQuery "github.com/PuerkitoBio/goquery"
	"github.com/PuerkitoBio/goquery"
	"regexp"
	//"sort"
	"strconv"
	//"io/ioutil"
	"time"
	//"io"
	"os"
	"strings"
	//"os/signal"
	//"labix.org/v2/mgo"
	//"labix.org/v2/mgo/bson"
	//"log"
	//"sync"
	"crypto/md5"
	"encoding/hex"
	//"labix.org/v2/mgo"
	//"labix.org/v2/mgo/bson"
	//"log"
	//"sync"
	"bufio"
	//"encoding/json"
)

var (
	// Презент
	Prezent_HASH      string = "cache/present-dv/hash"
	Prezent_HASH_CARD string = "cache/present-dv/hash_card"
	Prezent_LIST_UID  string = "cache/present-dv/uid"
	LimitUID          int    = 1000
	// URLs
	Prezent_materials_URL   string = "http://present-dv.ru/present/rubric/stroitelnye-i-otdelochnye-materialy?pageSize=200"
	Prezent_cars_URL        string = "http://present-dv.ru/present/rubric/avtomobili-spetstehnika-zapchasti-prodaja?pageSize=200"
	Prezent_autoservice_URL string = "http://present-dv.ru/present/rubric/avtoservis?pageSize=200"
	Prezent_tour_URL        string = "http://present-dv.ru/present/rubric/tury-puteshestviya-otdyh?page"
	Prezent_job_URL         string = "http://present-dv.ru/present/rubric/vakansii?pageSize=200"
	Prezent_flea_market_URL string = "http://present-dv.ru/present/rubric/torgovaya-ploshchadka-prodam?pageSize=200"
	Prezent_realty_URL      string = "http://present-dv.ru/present/rubric/nedvijimost-prodaja?pageSize=200"
	Prezent_CARD_URL        string = "http://present-dv.ru/present/notice/view/"

	Prezent_realty_CARD string = "cache/present-dv/realty_cards"
	Prezent_cars_CARD string = "cache/present-dv/cars_cards"
	Prezent_autoservice_CARD string = "cache/present-dv/autoservices_cards"
	Prezent_tour_CARD string = "cache/present-dv/tour_cards"
	Prezent_job_CARD string = "cache/present-dv/job_cards"
	Prezent_flea_CARD string = "cache/present-dv/flea_cards"
	Prezent_materials_CARD string = "cache/present-dv/materials_cards"

	channelUIDs  = make(chan string)
	channelCards = make(chan string)

	///// Podkova27.ru
	//Podkova_HASH      string = "cache/podkova27/hash"
	//Podkova_HASH_CARD string = "cache/podkova27/hash_card"
	//Podkova_LIST_UID  string = "cache/podkova27/uid"
	//Podkova_LIST_CARD string = "cache/podkova27/card"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func HashUsed(hash_file string) map[string]bool {
	used := make(map[string]bool)
	file, _ := os.Open(hash_file)
	defer file.Close()
	f := bufio.NewReader(file)
	for {
		read_line, _ := f.ReadString('\n')
		if read_line != "" {
			read_line = strings.Replace(read_line, "\n", "", 1)
			used[read_line] = true
		} else {
			//fmt.Println(used[read_line])
			//fmt.Println(used)
			return used
		}
	}
}

// Скрапер презента
func PresentScraper(URL string, List_File string) {
	start := time.Now()
	fmt.Println("Анализ начат:", URL)

	Pages, err := goquery.NewDocument(URL)
	check(err)

	if section := strings.TrimSpace(Pages.Find(".col-md-9").Text()); section != "" {

		used := HashUsed(Prezent_HASH)
		hash_string := GetMD5Hash(section)

		if !used[hash_string] {
			fmt.Println("Стартовая страница изменилась, затрачено:", time.Since(start).Seconds())

			// ДОРАБОТАТЬ !!! если всё на одной странице
			// Страницы со списками обьявлений
			List := Pages.Find(".page a").Last().Text()

			re := regexp.MustCompile("[0-9]+$")
			num, _ := strconv.Atoi(re.FindString(List))

			UIDs := UidCollector(num+1, URL)

			fmt.Println("Сбор Идентификаторов занял:", time.Since(start).Seconds())

			if len(UIDs) > 0 {
				fmt.Println("Получено идентификаторов:", len(UIDs))
				// Получение новых объявлений
				go func() {
					for uid := range UIDs {
						// Получить объявление в потоке
						go PrezentGetCard(uid, channelCards)
						time.Sleep(50 * time.Millisecond)
					}
				}()

				// hash list объявлений
				card_hash_file, _ := os.OpenFile(Prezent_HASH_CARD, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
				defer card_hash_file.Close()

				// html объявлений
				card_file, _ := os.OpenFile(List_File, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
				defer card_file.Close()

				count := 0
				good := 0
				for i := 0; i < len(UIDs); i++ {
					// Чтение потока объявлений
					Card := <-channelCards
					card_hash := GetMD5Hash(Card)

					card_used := HashUsed(Prezent_HASH_CARD)

					if !card_used[card_hash] {
						// запись хеша карточки
						card_hash_file.WriteString(card_hash + "\n")
						// сохранение карточки
						card_file.WriteString(Card + "\n")

						// Экспорт в JSON
						//PrezentExportCard(Card)

						good++

						fmt.Println(time.Since(start).Seconds(), "Получено:", good)
					} else {
						count++
						fmt.Println(time.Since(start).Seconds(), "Повтор:")
					}
				}

				fmt.Println("Затрачено:", time.Since(start).Seconds(), "Повторов:", count)

			}
			// все в порядке - заносим хеш в хранилище, и записываем его и цитату в файлы
			hash_list, _ := os.OpenFile(Prezent_HASH, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
			hash_list.WriteString(hash_string + "\n")
			hash_list.Close()

		} else {
			fmt.Println("Нет нового контента, затрачено:", time.Since(start).Seconds())
		}
	} else {
		fmt.Println("Не прочитаны данные, возможно поймана заглушка или произошли значительные изменния в структуре сайта")
	}
}

// Сборщик идентификаторов объявлений
func UidFinder(url string, stop chan bool, channelUIDs chan<- string) {

	select {
	case <-stop:
		return
	default:
		// Ветка default нужна, чтобы при отсутствии сообщений в chan работа функции продолжилась вместо блокировки на ожидании сообщения
	}

	Page, err := goquery.NewDocument(url)
	check(err)

	// Шаблон для поиска ссылок на объявления
	re := regexp.MustCompile("view/([0-9]+)$")
	UIDsRow := ""

	// Обработка ссылок
	Page.Find(".image-flex__wrapper").Each(
		func(i int, link *goquery.Selection) {
			url, _ := link.Attr("href")
			if re.FindString(url) != "" {
				if uid := re.FindStringSubmatch(url)[1]; re.FindString(url) != "" {
					UIDsRow += uid + ","
				}
			}
		})

	channelUIDs <- UIDsRow // отправка в канал
}

// Сбор списков обьявлений
func UidCollector(pages int, URL string) map[string]bool {
	stop := make(chan bool)
	UIDs := make(map[string]bool)

	used := HashUsed(Prezent_LIST_UID)

	// Начинаем с первой страницы
	// go UidFinder(Prezent_URL+"page/1", stop, channelUIDs) ///

	go func() {
		for i := 1; i < pages; i++ {
			url := URL + "&page=" + strconv.Itoa(i)

			go UidFinder(url, stop, channelUIDs) ///
		}
	}()

	fmt.Println("Запущено потоков:", pages)

	uid_list, _ := os.OpenFile(Prezent_LIST_UID, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	defer uid_list.Close()

	count := 0
	for i := 1; i < pages; i++ {

		UIDsRow := <-channelUIDs
		uids := strings.Split(UIDsRow, ",")

		for _, UID := range uids {
			if UID != "" && !UIDs[UID] {
				if !used[UID] {
					UIDs[UID] = true
					uid_list.WriteString(UID + "\n")
					fmt.Println("[NewUID]:", UID, len(UIDs))
				} else {
					count++
					fmt.Println("[isset]:", count)
				}

				if count == LimitUID {
					fmt.Println("Достигнут лимит повторов, собрано:", len(UIDs))
					return UIDs
					stop <- true
				}
			}
		}
	}
	return UIDs
}

func PrezentGetCard(uid string, channelCards chan<- string) {
	Page, err := goquery.NewDocument(Prezent_CARD_URL + uid)
	check(err)
	// Список страниц со списками обьявлений
	if Card, _ := Page.Find("main").Html(); Card != "" {
		Card = strings.Replace(Card, "\n", "", -1)

		channelCards <- Card // отправка в канал
	}
}

func main() {

	PresentScraper(Prezent_cars_URL, Prezent_cars_CARD)
	//
	PresentScraper(Prezent_autoservice_URL, Prezent_autoservice_CARD)
	//
	PresentScraper(Prezent_materials_URL, Prezent_materials_CARD)
	//
	PresentScraper(Prezent_tour_URL, Prezent_tour_CARD)
	//
	PresentScraper(Prezent_job_URL, Prezent_job_CARD)
	//
	PresentScraper(Prezent_flea_market_URL, Prezent_flea_CARD)
	//
	PresentScraper(Prezent_realty_URL, Prezent_realty_CARD)
}
