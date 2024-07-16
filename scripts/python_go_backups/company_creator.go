package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/icrowley/fake"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// ... rest of your code ...
type SoftwareCompany struct {
	Name      string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Industry  string `parquet:"name=industry, type=BYTE_ARRAY, convertedtype=UTF8"`
	Employees int32  `parquet:"name=employees, type=INT32"`
	Revenue   int64  `parquet:"name=revenue, type=INT64"`
	Location  string `parquet:"name=location, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type Employee struct {
	CompanyName string `parquet:"name=companyName, type=BYTE_ARRAY, convertedtype=UTF8"`
	FirstName   string `parquet:"name=firstName, type=BYTE_ARRAY, convertedtype=UTF8"`
	LastName    string `parquet:"name=lastName, type=BYTE_ARRAY, convertedtype=UTF8"`
	Email       string `parquet:"name=email, type=BYTE_ARRAY, convertedtype=UTF8"`
	Position    string `parquet:"name=position, type=BYTE_ARRAY, convertedtype=UTF8"`
	Salary      int32  `parquet:"name=salary, type=INT32"`
}

type Department struct {
	CompanyName    string `parquet:"name=companyName, type=BYTE_ARRAY, convertedtype=UTF8"`
	DepartmentName string `parquet:"name=departmentName, type=BYTE_ARRAY, convertedtype=UTF8"`
	ManagerID      int32  `parquet:"name=managerID, type=INT32"`
	Budget         int32  `parquet:"name=budget, type=INT32"`
	Location       string `parquet:"name=location, type=BYTE_ARRAY, convertedtype=UTF8"`
	StartDate      string `parquet:"name=startDate, type=BYTE_ARRAY, convertedtype=UTF8"`
	EndDate        string `parquet:"name=endDate, type=BYTE_ARRAY, convertedtype=UTF8"`
	DepartmentSize int32  `parquet:"name=departmentSize, type=INT32"`
	FunctionalArea string `parquet:"name=functionalArea, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func RandomDate() string {
	min := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0).Format("2006-01-02")
}

func main() {
	startTime := time.Now()
	rand.Seed(time.Now().UnixNano())

	var companySize int
	fmt.Println("Enter Amount of Companies to Create: ")
	fmt.Scan(&companySize)

	// Creating Companies
	companyFile, err := local.NewLocalFileWriter("companies_go.parquet")
	if err != nil {
		fmt.Println("Error creating company file:", err)
		return
	}

	companyWriter, err := writer.NewParquetWriter(companyFile, new(SoftwareCompany), 2)
	if err != nil {
		fmt.Println("Error creating company writer:", err)
		return
	}
	companyWriter.RowGroupSize = 128 * 1024 * 1024 //128M
	companyWriter.CompressionType = parquet.CompressionCodec_SNAPPY

	var companies []SoftwareCompany
	uniqueNames := make(map[string]bool)

	for i := 0; i < companySize; i++ {
		var name string
		attempts := 0
		for {
			attempts++
			name = fake.Company()
			if _, exists := uniqueNames[name]; !exists {
				break
			}
			if attempts >= 100 {
				name += fmt.Sprintf("-%d", rand.Intn(9999))
				break
			}
		}
		uniqueNames[name] = true

		company := SoftwareCompany{
			Name:      name,
			Industry:  fake.Industry(),
			Employees: int32(rand.Intn(20000)),
			Revenue:   int64(rand.Intn(1000000000)),
			Location:  fake.City(),
		}
		companies = append(companies, company)
		companyWriter.Write(&company)
	}
	companyWriter.WriteStop()
	companyFile.Close()

	// Creating Employees
	employeeFile, _ := local.NewLocalFileWriter("employees_go.parquet")
	employeeWriter, _ := writer.NewParquetWriter(employeeFile, new(Employee), 2)
	employeeWriter.RowGroupSize = 128 * 1024 * 1024 //128M
	employeeWriter.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, company := range companies {
		for i := 0; i < int(company.Employees); i++ {
			firstName := fake.FirstName()
			lastName := fake.LastName()
			email := fmt.Sprintf("%s.%s@%s.com", strings.ToLower(firstName), strings.ToLower(lastName), strings.ToLower(company.Name))
			employee := Employee{
				CompanyName: company.Name,
				FirstName:   firstName,
				LastName:    lastName,
				Email:       email,
				Position:    fake.JobTitle(),
				Salary:      int32(rand.Intn(150000)),
			}
			employeeWriter.Write(&employee)
		}
	}
	employeeWriter.WriteStop()
	employeeFile.Close()

	// Creating Departments
	departmentFile, _ := local.NewLocalFileWriter("departments_go.parquet")
	departmentWriter, _ := writer.NewParquetWriter(departmentFile, new(Department), 2)
	departmentWriter.RowGroupSize = 128 * 1024 * 1024 //128M
	departmentWriter.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, company := range companies {
		numDepartments := rand.Intn(5) + 1 // Random number of departments
		for i := 0; i < numDepartments; i++ {
			department := Department{
				CompanyName:    company.Name,
				DepartmentName: "Department of " + fake.Industry(),
				ManagerID:      int32(rand.Intn(999999)),
				Budget:         int32(rand.Intn(500000)),
				Location:       fake.City(),
				StartDate:      RandomDate(),
				EndDate:        RandomDate(),
				DepartmentSize: int32(rand.Intn(100)),
				FunctionalArea: fake.Industry(),
			}
			departmentWriter.Write(&department)
		}
	}
	departmentWriter.WriteStop()
	departmentFile.Close()

	totalTime := time.Since(startTime)
	fmt.Printf("Total time taken: %s\n", totalTime)
}