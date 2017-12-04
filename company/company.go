package company

import (
	"time"

	"github.com/guregu/dynamo"
)

type (
	//Company model
	Company struct {
		CompanyID string `dynamo:"CompanyId" bson:"-"`

		//CORPORATE_IDENTIFICATION_NUMBER
		CIN string `dynamo:"CIN" bson:"CIN"`

		//"DATE_OF_INCORPORATION"
		RegistrationDate time.Time `dynamo:"DOI,omitempty"`

		//"COMPANY_NAME"
		CompanyName string `dynamo:"CompanyName,omitempty"`

		//"COMPANY_STATUS"
		CompanyStatus string `dynamo:"Status,omitempty"`

		//"COMPANY_CLASS"
		CompanyClass string `dynamo:"Class,omitempty"`

		//"COMPANY_CATEGORY"
		CompanyCategory string `dynamo:"Category,omitempty"`

		//"AUTHORIZED_CAPITAL"
		AuthorizedCapital float64 `dynamo:"AuthorizedCapital,omitempty"`

		//"PAIDUP_CAPITAL"
		PaidupCapital float64 `dynamo:"PaidupCapital,omitempty"`

		//"REGISTERED_STATE"
		RegisteredState string `dynamo:"State,omitempty"`

		//"REGISTRAR_OF_COMPANIES"
		Registrar string `dynamo:"ROC,omitempty"`

		//"PRINCIPAL_BUSINESS_ACTIVITY"
		ActivityCode string `dynamo:"ActivityCode,omitempty"`

		//"REGISTERED_OFFICE_ADDRESS"
		OfficeAddress string `dynamo:"Address,omitempty"`

		//"SUB_CATEGORY"
		SubCategory string `dynamo:"SubCategory,omitempty"`

		//"EMAIL_ID"
		EmailID string `dynamo:"EmailId,omitempty"`

		//"LATEST_ANNUAL_REPORT_DATE"
		LatestAnnualReportDate time.Time `dynamo:"LatestAnnualReportDate,omitempty"`

		//"LATEST_BALANCE_SHEET_DATE"
		LatestBalanceSheetDate time.Time `dynamo:"LatestBalanceSheetDate,omitempty"`

		//"ACTIVITY_DESCRIPTION"
		ActivityDescription string `dynamo:"ActivityDescription,omitempty"`
	}

	//CompanyRaw model
	CompanyRaw struct {
		CompanyID string `dynamo:"-" bson:"CompanyId"`

		//CORPORATE_IDENTIFICATION_NUMBER
		CIN string `dynamo:"CIN" bson:"CIN"`

		//"DATE_OF_INCORPORATION"
		RegistrationDate string `dynamo:"RegistrationDate" bson:"DATE_OF_REGISTRATION"`

		//"COMPANY_NAME"
		CompanyName string `dynamo:"CompanyName" bson:"COMPANY_NAME"`

		//"COMPANY_STATUS"
		CompanyStatus string `dynamo:"Status" bson:"COMPANY_STATUS"`

		//"COMPANY_CLASS"
		CompanyClass string `dynamo:"Class" bson:"COMPANY_CLASS"`

		//"COMPANY_CATEGORY"
		CompanyCategory string `dynamo:"Category" bson:"COMPANY_CATEGORY"`

		//"AUTHORIZED_CAPITAL"
		AuthorizedCapital string `dynamo:"AuthorizedCapital" bson:"AUTHORIZED_CAPITAL"`

		//"PAIDUP_CAPITAL"
		PaidupCapital string `dynamo:"PaidupCapital" bson:"PAIDUP_CAPITAL"`

		//"REGISTERED_STATE"
		RegisteredState string `dynamo:"State" bson:"REGISTERED_STATE"`

		//"REGISTRAR_OF_COMPANIES"
		Registrar string `dynamo:"ROC" bson:"ROC"`

		//"PRINCIPAL_BUSINESS_ACTIVITY"
		ActivityCode string `dynamo:"ActivityCode" bson:"PRINCIPAL_BUSINESS_ACTIVITY_CODE"`

		//"REGISTERED_OFFICE_ADDRESS"
		OfficeAddress string `dynamo:"Address" bson:"REGISTERED_OFFICE_ADDRESS"`

		//"SUB_CATEGORY"
		SubCategory string `dynamo:"SubCategory" bson:"COMPANY_SUBCAT"`

		//"EMAIL_ID"
		EmailID string `dynamo:"EmailId" bson:"EMAIL_ID"`

		//"LATEST_ANNUAL_REPORT_DATE"
		LatestAnnualReportDate string `dynamo:"LatestAnnualReportDate" bson:"LATEST_ANNUAL_REPORT_DATE"`

		//"LATEST_BALANCE_SHEET_DATE"
		LatestBalanceSheetDate string `dynamo:"LatestBalanceSheetDate" bson:"LATEST_BALANCE_SHEET_DATE"`

		//"ACTIVITY_DESCRIPTION"
		ActivityDescription string `dynamo:"ActivityDescription" bson:"ACTIVITY_DESCRIPTION"`
	}
)

const companyTableName = "CompanyRaw"

//Put push company data to AWS
func Put(db *dynamo.DB, c CompanyRaw) (CompanyRaw, error) {
	return c, db.Table(companyTableName).Put(c).Run()

}

//PutBatch push company data to AWS
func PutBatch(db *dynamo.DB, c []CompanyRaw) error {
	bw := db.Table("Test").Batch("CompanyId").Write().Put(c)
	_, err := bw.Run()
	return err
}
