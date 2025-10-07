package main

import (
	"lang.yottadb.com/go/yottadb/v2"
	"fmt"
	"time"
	"strconv"
	"encoding/json"
	"net/http"
	"io/ioutil"
	"sync"
)

// https://mholt.github.io/json-to-go/
type randomusers struct {
	Results []struct {
		Gender string `json:"gender"`
		Name   struct {
			Title string `json:"title"`
			First string `json:"first"`
			Last  string `json:"last"`
		} `json:"name"`
		Location struct {
			Street struct {
				Number int    `json:"number"`
				Name   string `json:"name"`
			} `json:"street"`
			City        string `json:"city"`
			State       string `json:"state"`
			Country     string `json:"country"`
			Postcode    int    `json:"postcode"`
			Coordinates struct {
				Latitude  string `json:"latitude"`
				Longitude string `json:"longitude"`
			} `json:"coordinates"`
			Timezone struct {
				Offset      string `json:"offset"`
				Description string `json:"description"`
			} `json:"timezone"`
		} `json:"location"`
		Email string `json:"email"`
		Login struct {
			UUID     string `json:"uuid"`
			Username string `json:"username"`
			Password string `json:"password"`
			Salt     string `json:"salt"`
			Md5      string `json:"md5"`
			Sha1     string `json:"sha1"`
			Sha256   string `json:"sha256"`
		} `json:"login"`
		Dob struct {
			Date time.Time `json:"date"`
			Age  int       `json:"age"`
		} `json:"dob"`
		Registered struct {
			Date time.Time `json:"date"`
			Age  int       `json:"age"`
		} `json:"registered"`
		Phone string `json:"phone"`
		Cell  string `json:"cell"`
		ID    struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"id"`
		Picture struct {
			Large     string `json:"large"`
			Medium    string `json:"medium"`
			Thumbnail string `json:"thumbnail"`
		} `json:"picture"`
		Nat string `json:"nat"`
	} `json:"results"`
	Info struct {
		Seed    string `json:"seed"`
		Results int    `json:"results"`
		Page    int    `json:"page"`
		Version string `json:"version"`
	} `json:"info"`
}

func main() {
	defer yottadb.Shutdown(yottadb.MustInit())
	conn := yottadb.NewConn()

	//resp, err := http.Get("https://randomuser.me/api/?results=5000&seed=foobar")
	resp, err := http.Get("https://randomuser.me/api/?results=5000")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	fmt.Println("Response status:", resp.Status)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
    
	//fmt.Printf("%s\n", body)

	var users randomusers
	err = json.Unmarshal(body, &users)
	//fmt.Printf("%+v\n", users)
	var wg sync.WaitGroup
	for i := range users.Results {
		wg.Add(1)
		go saveV(&users, i, &wg)
	}
	wg.Wait()

	fmt.Println("--- Final Report ---")
	usersNode := conn.Node("^users")
	for userNode, key_number := range usersNode.Children() {
		_, err := strconv.Atoi(key_number)
		if err != nil {
			break
		}

		name		:= 	userNode.Child("name").Get()
		gender		:= 	userNode.Child("gender").Get()
		dob		:=	userNode.Child("dob").Get() 
		id 		:=      userNode.Child("id").Get()
		address		:=	userNode.Child("address").Get()
		city		:=	userNode.Child("city").Get()
		state		:=	userNode.Child("state").Get()
		country		:=	userNode.Child("country").Get()
		postcode	:=	userNode.Child("postcode").Get()
		line := name + " " + gender + " " + dob + " " + id + " " + address + " " + city + " " + state + " " + country + " " + postcode 
		fmt.Println(key_number, line)
	}

}

func saveV(users *randomusers, i int, wg *sync.WaitGroup) {
	    defer yottadb.QuitAfterFatalSignal()
	    v := users.Results[i]
	    first_name := v.Name.First
	    last_name  := v.Name.Last
	    gender     := v.Gender
	    dob        := v.Dob.Date.Format("2006-01-02")
	    id         := v.ID.Value
	    street     := fmt.Sprintf("%v %s", v.Location.Street.Number, v.Location.Street.Name)
	    city       := v.Location.City
	    state      := v.Location.State
	    country    := v.Location.Country
	    postcode   := v.Location.Postcode
	    name := first_name + " " + last_name

	    conn := yottadb.NewConn()
	    usersNode := conn.Node("^users")
	    if usersNode.Child("index", name).HasNone() { 
		   key_number := usersNode.Incr(1)
		   fmt.Printf("%s  -> %s %s %s %s %s\n", key_number, gender, first_name, last_name, dob, id)
		   fmt.Printf("    -> %s %s %s %s %v\n", street, city, state, country, postcode)

		   conn.Transaction("CS", []string{}, func() int {
			   usersNode.Child(key_number, "name").Set(name)
			   usersNode.Child(key_number, "gender").Set(gender)
			   usersNode.Child(key_number, "dob").Set(dob)
			   usersNode.Child(key_number, "id").Set(id)
			   usersNode.Child(key_number, "address").Set(street)
			   usersNode.Child(key_number, "city").Set(city)
			   usersNode.Child(key_number, "state").Set(state)
			   usersNode.Child(key_number, "country").Set(country)
			   usersNode.Child(key_number, "postcode").Set(postcode)
			   usersNode.Child("index", name, key_number).Set("")

			   // This intentionally causes a conflict
			   conn.Node("^userCount").Set(key_number)

			   // Dump the data
			   //fmt.Print(usersNode.Child(key_number).Dump())

		           return yottadb.YDB_OK
		   })
	    } else {
		    fmt.Println("found an existing user: " + name)
	    }
	    wg.Done()
}
