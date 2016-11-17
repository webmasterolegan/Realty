package present

import (
	"bufio"
	"fmt"
	"github.com/opesun/goquery"
	"regexp"
	//"sort"
	"strconv"
	"strings"
	//"io/ioutil"
	//"os"
	"time"
	//Пакеты, которые пригодятся для работы с файлами и сигналами:
	//"io"
	"os"
	//"os/signal"
	//А вот эти - для высчитывания хешей:
	"crypto/md5"
	"encoding/hex"
	//"labix.org/v2/mgo"
	//"labix.org/v2/mgo/bson"
	//"log"
	//"sync"
)

var (
	Prezent_HASH      string = "present-dv/hash"
	Prezent_HASH_CARD string = "present-dv/hash_card"
	Prezent_LIST_UID  string = "present-dv/uid"
	Prezent_LIST_CARD string = "present-dv/card"
	Prezent_CARD_DIR  string = "present-dv/cards/"
	LimitUID          int    = 10000
	// URLs
	Prezent_URL      string = "http://present-dv.ru/present/notice/index/rubric/nedvijimost-prodam/pageSize/200/"
	Prezent_CARD_URL string = "http://present-dv.ru/present/notice/view/"

	Start_Page   int = 1
	channelUIDs      = make(chan string)
	channelCards     = make(chan string)
	//UIDs         map[string]bool = make(map[string]bool) //map в котором в качестве ключей будем использовать строки, а для значений - булев тип.
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
func PresentScraper() {
	start := time.Now()
	fmt.Println("Анализ газеты презент начат")

	Pages, err := goquery.ParseUrl(Prezent_URL)
	check(err)

	if section := strings.TrimSpace(Pages.Find(".notices").Text()); section != "" {

		used := HashUsed(Prezent_HASH)
		hash_string := GetMD5Hash(section)

		if !used[hash_string] {
			fmt.Println("Изменилась стартовая страница, затрачено:", time.Since(start).Seconds())
			// все в порядке - заносим хеш в хранилище, и записываем его и цитату в файлы
			hash_list, _ := os.OpenFile(Prezent_HASH, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
			hash_list.WriteString(hash_string + "\n")
			hash_list.Close()

			// Список страниц со списками обьявлений
			List := Pages.Find(".pager a").Attrs("href")
			n := len(List) - 2
			re := regexp.MustCompile("[0-9]+$")
			num, _ := strconv.Atoi(re.FindString(List[n]))

			UIDs := UidCollector(num + 1)

			fmt.Println("Сбор Идентификаторов занял:", time.Since(start).Seconds())

			if len(UIDs) > 0 {
				fmt.Println("Получено идентификаторов:", len(UIDs))
				// Получение новых объявлений
				go func() {
					for uid := range UIDs {
						// Получить объявление в потоке
						go GetCard(uid, channelCards)
						time.Sleep(50 * time.Millisecond)
					}
				}()

				// hash list объявлений
				card_hash_file, _ := os.OpenFile(Prezent_HASH_CARD, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
				defer card_hash_file.Close()

				// html объявлений
				card_file, _ := os.OpenFile(Prezent_LIST_CARD, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
				defer card_file.Close()

				for i := 0; i < len(UIDs); i++ {
					// Чтение потока объявлений
					Card := <-channelCards
					card_hash := GetMD5Hash(Card)

					card_hash_file.WriteString(card_hash + "\n")
					card_file.WriteString(Card + "\n")

					fmt.Println("Спустя:", time.Since(start).Seconds(), "Получено:", i+1)
				}

			}

		} else {
			fmt.Println("Стартовая страница не изменилась, затрачено:", time.Since(start).Seconds())
		}
	} else {
		fmt.Println("Не прочитаны данные, поймана заглушка")
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

	Page, err := goquery.ParseUrl(url)
	check(err)

	// Сбор всех ссылок
	Links := Page.Find(".actions a").Attrs("href")
	// Шаблон для поиска ссылок на объявления
	re := regexp.MustCompile("view/([0-9]+)$")

	UIDsRow := ""
	// Сбор идентификаторов
	for _, link := range Links {
		if re.FindString(link) != "" {
			if uid := re.FindStringSubmatch(link)[1]; re.FindString(link) != "" {
				UIDsRow += uid + ","
			}
		}
	}
	channelUIDs <- UIDsRow // отправка в канал
}

// Сбор списков обьявлений
func UidCollector(pages int) map[string]bool {
	stop := make(chan bool)
	UIDs := make(map[string]bool)

	used := HashUsed(Prezent_LIST_UID)

	// Начинаем с первой страницы
	// go UidFinder(Prezent_URL+"page/1", stop, channelUIDs) ///

	go func() {
		for i := 1; i < pages; i++ {
			url := Prezent_URL + "page/" + strconv.Itoa(i)

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
				} else {
					count++
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

func GetCard(uid string, channelCards chan<- string) {
	Page, err := goquery.ParseUrl(Prezent_CARD_URL + uid)
	check(err)
	// Список страниц со списками обьявлений
	if Card := strings.TrimSpace(Page.Find(".notice-card").Html()); Card != "" {
		Card = strings.Replace(Card, "\n", "", -1)

		channelCards <- Card // отправка в канал
	}
}
