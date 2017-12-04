package mca

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/gocarina/gocsv"
	"github.com/guregu/dynamo"
	"vivriticapital.com/synd/company"
	"vivriticapital.com/synd/utils"
)

//MCACompany Model
type MCACompany struct {
	CIN                       string `csv:"CORPORATE_IDENTIFICATION_NUMBER"`
	DateOfRegistration        string `csv:"DATE_OF_REGISTRATION"`
	CompanyName               string `csv:"COMPANY_NAME"`
	CompanyStatus             string `csv:"COMPANY_STATUS"`
	CompanyClass              string `csv:"COMPANY_CLASS"`
	CompanyCategory           string `csv:"COMPANY_CATEGORY"`
	AuthorizedCapital         string `csv:"AUTHORIZED_CAPITAL"`
	PaidupCapital             string `csv:"PAIDUP_CAPITAL"`
	RegisteredState           string `csv:"REGISTERED_STATE"`
	RegistrarOfCompanies      string `csv:"REGISTRAR_OF_COMPANIES"`
	PrincipalBusinessActivity string `csv:"PRINCIPAL_BUSINESS_ACTIVITY"`
	RegisteredOfficeAddress   string `csv:"REGISTERED_OFFICE_ADDRESS"`
	SubCategory               string `csv:"SUB_CATEGORY"`
}

//ParseMcaCsvData reads the MCA CSV file data to struct
func ParseMcaCsvData(mcaFileURL string) ([]*MCACompany, error) {

	mc := []*MCACompany{}
	mcaFile, err := os.OpenFile(mcaFileURL, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer mcaFile.Close()
	gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
		//return csv.NewReader(in)
		return gocsv.LazyCSVReader(in) // Allows use of quotes in CSV
	})
	if err := gocsv.UnmarshalFile(mcaFile, &mc); err != nil {
		return nil, fmt.Errorf("error while parsing MCA CSV file: %v", err)
	}
	return mc, nil
}

func uploadMCACompany(db *dynamo.DB, c []*MCACompany, wg *sync.WaitGroup) {
	defer wg.Done()
	for i, p := range c {
		fmt.Printf("\rInserting %d of %d", i+1, len(c))
		var authorizedCapital, paidupCapital float64
		authorizedCapital, _ = strconv.ParseFloat(strings.Replace(p.AuthorizedCapital, ",", "", -1), 64)
		paidupCapital, _ = strconv.ParseFloat(strings.Replace(p.PaidupCapital, ",", "", -1), 64)

		_, err := company.Put(db, company.Company{CIN: p.CIN, RegistrationDate: p.DateOfRegistration,
			CompanyName:       p.CompanyName,
			CompanyStatus:     p.CompanyStatus,
			CompanyClass:      p.CompanyClass,
			CompanyCategory:   p.CompanyCategory,
			AuthorizedCapital: authorizedCapital,
			PaidupCapital:     paidupCapital,
			RegisteredState:   p.RegisteredState,
			Registrar:         p.RegistrarOfCompanies,
			ActivityCode:      p.PrincipalBusinessActivity,
			OfficeAddress:     p.RegisteredOfficeAddress,
			SubCategory:       p.SubCategory})
		if err != nil {
			fmt.Printf("error while posting to dynamo: %v\n", err)
		}
	}
}

func getMCAFiles() []string {
	return []string{"http://www.mca.gov.in/Ministry/pdf/Andaman_Nicobar_Islands_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Arunachal_Pradesh_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Assam_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Bihar_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Chandigarh_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Chattisgarh_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Dadar_Nagar_Haveli_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Daman_and_Diu_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Delhi_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Goa_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Gujarat_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Haryana_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Himachal_Pradesh_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Jammu_and_Kashmir_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Jharkhand_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Karnataka_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Kerala_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Lakshadweep_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Madhya Pradesh_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Maharastra_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Manipur_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Meghalaya_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Mizoram_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Nagaland_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Odisha_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Puducherry_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Punjab_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Rajasthan_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Sikkim_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Tamil_Nadu_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Telangana_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Tripura_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Uttar_Pradesh_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/Uttarakhand_2016.xlsx",
		"http://www.mca.gov.in/Ministry/pdf/West_Bengal_2016.xlsx"}
	// return []string{"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Andaman_Nicobar.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Andhra_Pradesh.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Arunachal_Pradesh.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Assam.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Bihar.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Chandigarh.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Chhattisgarh.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Dadra_Nagar_Haveli.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Daman_Diu.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Delhi.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Goa.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Gujarat.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Haryana.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Himachal.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Jammu.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Jharkhand.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Karnataka.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Kerala.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Lakshadweep.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Madhya_Pradesh.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Maharashtra.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Manipur.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Meghalaya.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Mizoram.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Nagaland.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Odisha.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Puducherry.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Punjab.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Rajasthan.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Tamil_Nadu.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Telangana.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Tripura.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Uttar_Pradesh.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_Uttarakhand.csv",
	// 	"https://data.gov.in/sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_West_Bengal.csv"}
}

func parsingMCACSV(db *dynamo.DB) {
	var wg sync.WaitGroup

	outputFolder := "E:\\data\\company_data_from_mca\\"
	mcaDataFiles := getMCAFiles()

	fmt.Println("Downloading MCA files...")
	wg.Add(len(mcaDataFiles))
	for _, f := range mcaDataFiles {
		go utils.DownloadFile(fmt.Sprintf("%s%s", outputFolder, filepath.Base(f)), f, &wg)
	}
	wg.Wait()
	fmt.Println("Downloaded")

	mcacsvfiles, err := ioutil.ReadDir(outputFolder)
	if err != nil {
		fmt.Println("Couldn't able to read the MCA folder.")
	}

	for _, f := range mcacsvfiles {
		fmt.Printf("Parsing file: %s\n", f.Name())
		c, err := ParseMcaCsvData(fmt.Sprintf("%s%s", outputFolder, f.Name()))
		if err != nil {
			fmt.Printf("error parse mca csv file: %v\n", err)
		}
		go uploadMCACompany(db, c, &wg)
	}

}
