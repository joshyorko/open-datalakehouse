package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/icrowley/fake"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/schollz/progressbar/v3"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

var randSource *rand.Rand

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

	sec := randSource.Int63n(delta) + min
	return time.Unix(sec, 0).Format("2006-01-02")
}

// MinIO S3 Upload Function with retry logic
func uploadToMinIO(ctx context.Context, client *minio.Client, bucketName string, key string, data []byte, wg *sync.WaitGroup, semaphore chan struct{}) {
	defer wg.Done()
	defer func() { <-semaphore }()
	const maxRetries = 3
	for i := 0; i < maxRetries; i++ {
		_, err := client.PutObject(
			ctx,
			bucketName,
			key,
			bytes.NewReader(data),
			int64(len(data)),
			minio.PutObjectOptions{ContentType: "application/parquet"},
		)
		if err == nil {
			return
		}
		log.Printf("Failed to upload data to MinIO: %v (attempt %d/%d)", err, i+1, maxRetries)
		time.Sleep(time.Duration(i+1) * time.Second) // Exponential backoff
	}
	log.Printf("Failed to upload data to MinIO after %d attempts", maxRetries)
}

func main() {
	startTime := time.Now()
	randSource = rand.New(rand.NewSource(time.Now().UnixNano()))

	// MinIO Client Setup
	endpoint := "192.168.1.83:9000"
	accessKey := "minio-admin"
	secretAccessKey := "Pa22word22"
	useSSL := false

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalf("Failed to create MinIO client: %v", err)
	}

	// Create a context
	ctx := context.Background()

	// Bucket and Database Names
	bucketName := "upload" // Your S3 bucket name
	databaseName := "bronze-data"                // Database name for the file path

	var companySize int
	fmt.Println("Enter Amount of Companies to Create: ")
	fmt.Scan(&companySize)

	var companies []SoftwareCompany
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10) // Limit to 10 concurrent uploads

	// Creating Companies
	func() {
		companyData := new(bytes.Buffer)
		companyFile := writerfile.NewWriterFile(companyData)
		companyWriter, err := writer.NewParquetWriter(companyFile, new(SoftwareCompany), 2)
		if err != nil {
			fmt.Println("Error creating company writer:", err)
			return
		}
		defer companyFile.Close()

		companyWriter.RowGroupSize = 128 * 1024 * 1024 // 128M
		companyWriter.CompressionType = parquet.CompressionCodec_SNAPPY

		uniqueNames := make(map[string]bool)

		// Progress bar for companies
		bar := progressbar.NewOptions(companySize, progressbar.OptionSetDescription("Creating Companies"))

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
					name += fmt.Sprintf("-%d", randSource.Intn(9999))
					break
				}
			}
			uniqueNames[name] = true

			company := SoftwareCompany{
				Name:      name,
				Industry:  fake.Industry(),
				Employees: int32(randSource.Intn(250000)), // Reduced for demo purposes
				Revenue:   int64(randSource.Intn(1000000000)),
				Location:  fake.City(),
			}
			companies = append(companies, company)
			companyWriter.Write(&company)
			bar.Add(1)
		}

		companyWriter.WriteStop()

		// Upload Companies to MinIO
		companyFilename := fmt.Sprintf("%s/companies/companies-go-%s.parquet", databaseName, time.Now().Format("20060102-150405"))
		wg.Add(1)
		semaphore <- struct{}{}
		go uploadToMinIO(ctx, minioClient, bucketName, companyFilename, companyData.Bytes(), &wg, semaphore)
	}()

	// Ensure all companies are created before proceeding
	wg.Wait()

	// Create and upload employees and departments concurrently
	employeeBar := progressbar.NewOptions(1, progressbar.OptionSetDescription("Creating Employees"))

	for _, company := range companies {
		company := company // Capture range variable
		wg.Add(1)
		go func() {
			defer wg.Done()
			employeeData := new(bytes.Buffer)
			employeeFile := writerfile.NewWriterFile(employeeData)
			employeeWriter, _ := writer.NewParquetWriter(employeeFile, new(Employee), 2)
			employeeWriter.RowGroupSize = 128 * 1024 * 1024 // 128M
			employeeWriter.CompressionType = parquet.CompressionCodec_SNAPPY

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
					Salary:      int32(randSource.Intn(150000)),
				}
				employeeWriter.Write(&employee)
				employeeBar.Add(1)
			}
			employeeWriter.WriteStop()
			employeeFile.Close()

			// Upload Employees to MinIO
			employeeFilename := fmt.Sprintf("%s/employees/employees-go-%s-%s.parquet", databaseName, company.Name, time.Now().Format("20060102-150405"))
			wg.Add(1)
			semaphore <- struct{}{}
			go uploadToMinIO(ctx, minioClient, bucketName, employeeFilename, employeeData.Bytes(), &wg, semaphore)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			departmentData := new(bytes.Buffer)
			departmentFile := writerfile.NewWriterFile(departmentData)
			departmentWriter, _ := writer.NewParquetWriter(departmentFile, new(Department), 2)
			departmentWriter.RowGroupSize = 128 * 1024 * 1024 // 128M
			departmentWriter.CompressionType = parquet.CompressionCodec_SNAPPY

			numDepartments := randSource.Intn(5) + 1 // Random number of departments
			departmentBar := progressbar.NewOptions(numDepartments, progressbar.OptionSetDescription(fmt.Sprintf("Creating Departments for %s", company.Name)))

			for i := 0; i < numDepartments; i++ {
				department := Department{
					CompanyName:    company.Name,
					DepartmentName: "Department of " + fake.Industry(),
					ManagerID:      int32(randSource.Intn(999999)),
					Budget:         int32(randSource.Intn(500000)),
					Location:       fake.City(),
					StartDate:      RandomDate(),
					EndDate:        RandomDate(),
					DepartmentSize: int32(randSource.Intn(100)),
					FunctionalArea: fake.Industry(),
				}
				departmentWriter.Write(&department)
				departmentBar.Add(1)
			}
			departmentWriter.WriteStop()
			departmentFile.Close()

			// Upload Departments to MinIO
			departmentFilename := fmt.Sprintf("%s/departments/departments-go-%s-%s.parquet", databaseName, company.Name, time.Now().Format("20060102-150405"))
			wg.Add(1)
			semaphore <- struct{}{}
			go uploadToMinIO(ctx, minioClient, bucketName, departmentFilename, departmentData.Bytes(), &wg, semaphore)
		}()
	}

	wg.Wait() // Wait for all uploads to finish

	totalTime := time.Since(startTime)
	fmt.Printf("Total time taken: %s\n", totalTime)
}
