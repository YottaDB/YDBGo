package main

import (
	"lang.yottadb.com/go/yottadb"
	"fmt"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"time"
	"strconv"
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

func ec(err error, errstr yottadb.BufferT) {
    if err != nil {
	    errval, _ := errstr.ValStr(yottadb.NOTTP, nil)
	    fmt.Printf("Error encountered! %s\n", errval)
	    panic(errval)
    }
}

func main() {
    defer yottadb.Exit()
    var errstr yottadb.BufferT
    defer errstr.Free()

    errstr.Alloc(255)
    tptoken := yottadb.NOTTP
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
    for i, _ := range users.Results {
	    wg.Add(1)
	    go saveV(&users, i, &wg)
    }
    wg.Wait()

    fmt.Println("--- Final Report ---")
    key_number := "" 
    for {
	    // $Order
	    key_number, err = yottadb.SubNextE(tptoken, &errstr, "^users", []string{key_number})
	    // End of loop
	    if err != nil {
		    break
	    }
	    // We got to the index
	    if _, err := strconv.Atoi(key_number); err != nil {
		    break
	    }
	    
	    name, _ := yottadb.ValE(tptoken, &errstr, "^users", []string{key_number, "name"})
	    gender, _ := yottadb.ValE(tptoken, &errstr, "^users", []string{key_number, "gender"})
	    dob, _ := yottadb.ValE(tptoken, &errstr, "^users", []string{key_number, "dob"})
	    id, _ := yottadb.ValE(tptoken, &errstr, "^users", []string{key_number, "id"})
	    address, _ := yottadb.ValE(tptoken, &errstr, "^users", []string{key_number, "address"})
	    city, _ := yottadb.ValE(tptoken, &errstr, "^users", []string{key_number, "city"})
	    state, _ := yottadb.ValE(tptoken, &errstr, "^users", []string{key_number, "state"})
	    country, _ := yottadb.ValE(tptoken, &errstr, "^users", []string{key_number, "country"})
	    postcode, _ := yottadb.ValE(tptoken, &errstr, "^users", []string{key_number, "postcode"})
	    line := key_number + ": " + name + " " + gender + " " + dob + " " + id + " " + address + " " + city + " " + state + " " + country + " " + postcode 
	    fmt.Println(line)
    }

}
func saveV(users *randomusers, i int, wg *sync.WaitGroup) {
	var errstr yottadb.BufferT
	defer errstr.Free()

	errstr.Alloc(255)
	tptoken := yottadb.NOTTP

	    v := users.Results[i]
	    first_name := v.Name.First  //done
	    last_name  := v.Name.Last   //done
	    gender     := v.Gender      //done
	    dob        := v.Dob.Date.Format("2006-01-02") //done
	    id         := v.ID.Value    //done
	    street     := fmt.Sprintf("%v %s", v.Location.Street.Number, v.Location.Street.Name)
	    city       := v.Location.City
	    state      := v.Location.State
	    country    := v.Location.Country
	    postcode   := v.Location.Postcode

	    name := first_name + " " + last_name

	    data, err := yottadb.DataE(tptoken, &errstr, "^users", []string{"index",name})
	    ec(err, errstr)
	    if data == 0 {
		    key_number, err := yottadb.IncrE(tptoken, &errstr, strconv.Itoa(1), "^users", []string{})
		    ec(err, errstr)

		    fmt.Printf("%v  -> %s %s %s %s %s\n", key_number, gender, first_name, last_name, dob, id)
		    fmt.Printf("    -> %s %s %s %s %v\n", street, city, state, country, postcode)

		    err = yottadb.TpE(yottadb.NOTTP, &errstr, func(tptoken uint64, errptr *yottadb.BufferT) int32 {
			    err = yottadb.SetValE(tptoken, &errstr, name, "^users", []string{key_number, "name"})
			    if err != nil {
				fmt.Printf("**%s** ", err)
				return int32(yottadb.ErrorCode(err))
			    }
			    err = yottadb.SetValE(tptoken, &errstr, gender, "^users", []string{key_number, "gender"})
			    if err != nil {
				fmt.Printf("**%s** ", err)
				return int32(yottadb.ErrorCode(err))
			    }
			    err = yottadb.SetValE(tptoken, &errstr, dob, "^users", []string{key_number, "dob"})
			    if err != nil {
				fmt.Printf("**%s** ", err)
				return int32(yottadb.ErrorCode(err))
			    }
			    err = yottadb.SetValE(tptoken, &errstr, id, "^users", []string{key_number, "id"})
			    if err != nil {
				fmt.Printf("**%s** ", err)
				return int32(yottadb.ErrorCode(err))
			    }
			    err = yottadb.SetValE(tptoken, &errstr, street, "^users", []string{key_number, "address"})
			    if err != nil {
				fmt.Printf("**%s** ", err)
				return int32(yottadb.ErrorCode(err))
			    }
			    err = yottadb.SetValE(tptoken, &errstr, city, "^users", []string{key_number, "city"})
			    if err != nil {
				fmt.Printf("**%s** ", err)
				return int32(yottadb.ErrorCode(err))
			    }
			    err = yottadb.SetValE(tptoken, &errstr, state, "^users", []string{key_number, "state"})
			    if err != nil {
				fmt.Printf("**%s** ", err)
				return int32(yottadb.ErrorCode(err))
			    }
			    err = yottadb.SetValE(tptoken, &errstr, country, "^users", []string{key_number, "country"})
			    if err != nil {
				fmt.Printf("**%s** ", err)
				return int32(yottadb.ErrorCode(err))
			    }
			    err = yottadb.SetValE(tptoken, &errstr, strconv.Itoa(postcode), "^users", []string{key_number, "postcode"})
			    if err != nil {
				fmt.Printf("**%s** ", err)
				return int32(yottadb.ErrorCode(err))
			    }
			    err = yottadb.SetValE(tptoken, &errstr, "", "^users", []string{"index", name, key_number})
			    if err != nil {
				fmt.Printf("**%s** ", err)
				return int32(yottadb.ErrorCode(err))
			    }
			    err = yottadb.SetValE(tptoken, &errstr, key_number, "^userCount", []string{})
			    if err != nil {
				fmt.Printf("**%s** ", err)
				return int32(yottadb.ErrorCode(err))
			    }
			    return yottadb.YDB_OK
		    }, "CS", []string{})
		    ec(err, errstr)
	    } else {
		    fmt.Println("found an existing user: " + name)
	    }
	    wg.Done()
}
