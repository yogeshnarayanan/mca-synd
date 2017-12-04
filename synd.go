package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"vivriticapital.com/synd/company"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/guregu/dynamo"
)

func main() {
	/* db := getDynamoSession()
	if err := pushMgoToDynamo(db); err != nil {
		fmt.Printf("error pushing to dynamo: %v", err)
	} */
	if err := sendSqsCinSync(); err != nil {
		fmt.Printf("error pushing to dynamo: %v", err)
	}

}

func sendSqsCinSync() error {
	var e error
	mgoSession, err := getMgoSession()
	if err != nil {
		return fmt.Errorf("Couldn't able to connect to mongo, hence returning:%v", err)

	}
	termte := make(chan string, 1)
	contu := make(chan string, 1)

	for {
		var c []company.CompanyRaw
		err = mgoSession.DB("vvc").C("company").Find(bson.M{"SERVER_SYNC": false}).Limit(1000).All(&c)
		if err != nil {
			e = fmt.Errorf("error retrieving data from company :%v", err)
			break
		}
		if len(c) == 0 {
			break
		}
		for k, v := range sendSqsCin(c) {
			err := mgoSession.DB("vvc").C("company").Update(bson.M{"CIN": k}, bson.M{"$set": bson.M{"SERVER_SYNC": true, "Remark": v}})
			if err != nil {
				e = fmt.Errorf("error update response back to mgo :%v", err)
				break
			}
		}
		go func() {
			time.Sleep(5 * time.Second)
			contu <- "Continue"
		}()

		go func() {
			var input string
			fmt.Println("\nPress any key and enter to terminate...")
			fmt.Scan(&input)
			termte <- input
		}()
		select {
		case <-termte:
			fmt.Println("Terminated")
			os.Exit(0)
		case <-contu:
			fmt.Println("Continue processing...")
		}
	}
	if e != nil {
		return e
	}
	return nil
}

func sendSqsCin(c []company.CompanyRaw) map[string]string {
	var result map[string]string
	result = make(map[string]string)

	svc := sqs.New(session.New(), &aws.Config{Region: aws.String("ap-south-1")})

	// URL to our queue
	qURL := "https://sqs.ap-south-1.amazonaws.com/078188256006/visy-mca-cin-queue"

	for i, cc := range c {
		fmt.Printf("\n Adding %d of %d to SQS...", i+1, len(c))
		out, err := json.Marshal(struct{ CIN string }{cc.CIN})
		if err != nil {
			result[cc.CIN] = err.Error()
			continue
		}
		msgResult, err := svc.SendMessage(&sqs.SendMessageInput{
			DelaySeconds: aws.Int64(0),
			MessageBody:  aws.String(string(out)),
			QueueUrl:     &qURL,
		})

		if err != nil {
			result[cc.CIN] = err.Error()
			continue
		}
		result[cc.CIN] = ""
		fmt.Printf("\nAdded to SQS...%s", *msgResult.MessageId)
	}
	return result
}

func pushMgoToDynamo(db *dynamo.DB) error {
	var e error
	mgoSession, err := getMgoSession()
	if err != nil {
		return fmt.Errorf("Couldn't able to connect to mongo, hence returning:%v", err)

	}
	termte := make(chan string, 1)
	contu := make(chan string, 1)

	for {
		var c []company.CompanyRaw
		err = mgoSession.DB("vvc").C("company").Find(bson.M{"SERVER_SYNC": false}).Limit(1000).All(&c)
		if err != nil {
			e = fmt.Errorf("error retrieving data from company :%v", err)
			break
		}
		if len(c) == 0 {
			break
		}
		for k, v := range uploadRawCompany(db, c) {
			err := mgoSession.DB("vvc").C("company").Update(bson.M{"CIN": k}, bson.M{"$set": bson.M{"SERVER_SYNC": true, "Remark": v}})
			if err != nil {
				e = fmt.Errorf("error update response back to mgo :%v", err)
				break
			}
		}
		go func() {
			time.Sleep(5 * time.Second)
			contu <- "Continue"
		}()

		go func() {
			var input string
			fmt.Println("\nPress any key and enter to terminate...")
			fmt.Scan(&input)
			termte <- input
		}()
		select {
		case <-termte:
			fmt.Println("Terminated")
			os.Exit(0)
		case <-contu:
			fmt.Println("Continue processing...")
		}
	}
	if e != nil {
		return e
	}
	return nil
}
func uploadRawCompanyBatch(db *dynamo.DB, c []company.CompanyRaw) map[string]string {
	var result map[string]string
	result = make(map[string]string)
	err := company.PutBatch(db, c)
	if err != nil {
		for _, p := range c {
			result[p.CIN] = err.Error()
		}
	}
	return result
}

func uploadRawCompany(db *dynamo.DB, c []company.CompanyRaw) map[string]string {
	var result map[string]string
	result = make(map[string]string)
	for i, p := range c {
		fmt.Printf("\rInserting %d of %d", i+1, len(c))

		/* var authorizedCapital, paidupCapital float64
		authorizedCapital, _ = strconv.ParseFloat(strings.Replace(p.AuthorizedCapital, ",", "", -1), 64)
		paidupCapital, _ = strconv.ParseFloat(strings.Replace(p.PaidupCapital, ",", "", -1), 64) */

		result[p.CIN] = ""
		_, err := company.Put(db, company.CompanyRaw{CIN: p.CIN,
			RegistrationDate:       p.RegistrationDate,
			CompanyName:            p.CompanyName,
			CompanyStatus:          p.CompanyStatus,
			CompanyClass:           p.CompanyClass,
			CompanyCategory:        p.CompanyCategory,
			AuthorizedCapital:      p.AuthorizedCapital,
			PaidupCapital:          p.PaidupCapital,
			RegisteredState:        p.RegisteredState,
			Registrar:              p.Registrar,
			ActivityCode:           p.ActivityCode,
			OfficeAddress:          p.OfficeAddress,
			SubCategory:            p.SubCategory,
			EmailID:                p.EmailID,
			LatestAnnualReportDate: p.LatestAnnualReportDate,
			LatestBalanceSheetDate: p.LatestBalanceSheetDate,
			ActivityDescription:    p.LatestBalanceSheetDate})

		result[p.CIN] = ""
		_, err = company.Put(db, p)
		if err != nil {
			result[p.CIN] = err.Error()
		}

	}
	return result
}

func getDynamoSession() *dynamo.DB {
	return dynamo.New(session.New(), &aws.Config{
		Region:      aws.String("ap-south-1"),
		Credentials: credentials.NewSharedCredentials("", "default")})
}

func getMgoSession() (*mgo.Session, error) {
	var s *mgo.Session
	var err error
	s, err = mgo.Dial("mongodb://localhost:27017/vvc")
	if err != nil {
		return s, err
	}
	fmt.Println("mongodb connection has opened...")
	return s, nil
}
