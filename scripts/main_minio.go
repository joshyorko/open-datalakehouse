package main

import (
    "bytes"
    "encoding/csv"
    "fmt"
    "log"
    "math/rand"
    "strings"
    "time"

    "github.com/minio/minio-go/v7"
    "github.com/minio/minio-go/v7/pkg/credentials"
    "github.com/icrowley/fake"
)

// Structs
type SoftwareCompany struct {
    Name      string
    Industry  string
    Employees int
    Revenue   int
    Location  string
}

type Employee struct {
    CompanyName string
    StartDate   string
    FirstName   string
    LastName    string
    Email       string
    Position    string
    Salary      int
}

type Department struct {
    CompanyName    string
    DepartmentName string
    ManagerID      int
    Budget         int
    Location       string
    StartDate      string
    EndDate        string
    DepartmentSize int
    FunctionalArea string
}

// MinIO S3 Upload Function
func uploadToMinIO(client *minio.Client, bucketName string, key string, data []byte) error {
    _, err := client.PutObject(
        bucketName,
        key,
        bytes.NewReader(data),
        int64(len(data)),
        minio.PutObjectOptions{ContentType: "application/csv"},
    )
    return err
}

func RandomDate() string {
    min := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
    max := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
    delta := max - min

    sec := rand.Int63n(delta) + min
    return time.Unix(sec, 0).Format("2006-01-02")
}

// Function to generate a unique filename with a timestamp
func uniqueFilename(prefix string) string {
    timestamp := time.Now().Format("20060102-150405") // YYYYMMDD-HHMMSS format
    return fmt.Sprintf("%s-%s.csv", prefix, timestamp)
}

func main() {
    startTime := time.Now()
    rand.Seed(time.Now().UnixNano())

    // MinIO Client
    endpoint := "your-minio-endpoint:9000"
    accessKeyID := "your-access-key"
    secretAccessKey := "your-secret-key"
    useSSL := false

    minioClient, err := minio.New(endpoint, &minio.Options{
        Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
        Secure: useSSL,
    })
    if err != nil {
        log.Fatalf("Failed to create MinIO client: %v", err)
    }

    // Bucket and Database Names
    bucketName := "portfolio-company-datalake-jy" // Your S3 bucket name
    databaseName := "bronze-data"                // Database name for the file path

    // Use the uniqueFilename function to generate filenames
    companyFilename := uniqueFilename("companies-data")
    employeeFilename := uniqueFilename("employees-data")
    departmentFilename := uniqueFilename("departments-data")

    // Generating Companies
    var companySize int
    fmt.Println("Enter Amount of Companies to Create: ")
    fmt.Scan(&companySize)

    var companies []SoftwareCompany
    uniqueNames := make(map[string]bool)

    companyData := new(bytes.Buffer)
    companyWriter := csv.NewWriter(companyData)
    companyWriter.Write([]string{"Name", "Industry", "Employees", "Revenue", "Location"})

    for i := 0; i < companySize; i++ {
        var name string
        for {
            name = fake.Company()
            if _, exists := uniqueNames[name]; !exists {
                break
            }
        }
        uniqueNames[name] = true

        company := SoftwareCompany{
            Name:      name,
            Industry:  fake.Industry(),
            Employees: rand.Intn(20000),
            Revenue:   rand.Intn(1000000000),
            Location:  fake.City(),
        }
        companies = append(companies, company)
        companyWriter.Write([]string{company.Name, company.Industry, fmt.Sprint(company.Employees), fmt.Sprint(company.Revenue), company.Location})
    }
    companyWriter.Flush()

    // Upload Companies to MinIO
    if err := uploadToMinIO(minioClient, bucketName, fmt.Sprintf("%s/companies/%s", databaseName, companyFilename), companyData.Bytes()); err != nil {
        log.Fatalf("Failed to upload company data to MinIO: %v", err)
    }

    companyTime := time.Since(startTime)
    fmt.Printf("Time taken for creating companies: %s\n", companyTime)

    // Generating Employees
    employeeData := new(bytes.Buffer)
    employeeWriter := csv.NewWriter(employeeData)
    employeeWriter.Write([]string{"Company_Name", "First_Name", "Last_Name", "Email", "Position", "Salary"})

    for _, company := range companies {
        domain := strings.Replace(strings.ToLower(company.Name), " ", "", -1)
        for i := 0; i < company.Employees; i++ {
            firstName := fake.FirstName()
            lastName := fake.LastName()
            email := fmt.Sprintf("%s.%s@%s.com", strings.ToLower(firstName), strings.ToLower(lastName), domain)

            employee := Employee{
                CompanyName: company.Name,
                StartDate:   RandomDate(),
                FirstName:   firstName,
                LastName:    lastName,
                Email:       email,
                Position:    fake.JobTitle(),
                Salary:      rand.Intn(150000),
            }

            employeeWriter.Write([]string{employee.CompanyName, employee.FirstName, employee.LastName, employee.Email, employee.Position, fmt.Sprint(employee.Salary)})
        }
    }
    employeeWriter.Flush()

    // Upload Employees to MinIO
    if err := uploadToMinIO(minioClient, bucketName, fmt.Sprintf("%s/employees/%s", databaseName, employeeFilename), employeeData.Bytes()); err != nil {
        log.Fatalf("Failed to upload employee data to MinIO: %v", err)
    }

    // Generating Departments
    departmentData := new(bytes.Buffer)
    departmentWriter := csv.NewWriter(departmentData)
    departmentWriter.Write([]string{"Company_Name", "Department_Name", "Manager_ID", "Budget", "Location", "Start_Date", "End_Date", "Department_Size", "Functional_Area"})

    for _, company := range companies {
        for i := 0; i < rand.Intn(5)+1; i++ { // Random number of departments per company
            department := Department{
                CompanyName:    company.Name,
                DepartmentName: "Department of " + fake.Industry(),
                ManagerID:      rand.Intn(999999),
                Budget:         rand.Intn(500000),
                Location:       fake.City(),
                StartDate:      RandomDate(),
                EndDate:        RandomDate(),
                DepartmentSize: rand.Intn(100),
                FunctionalArea: fake.Industry(),
            }
            departmentWriter.Write([]string{department.CompanyName, department.DepartmentName, fmt.Sprint(department.ManagerID), fmt.Sprint(department.Budget), department.Location, department.StartDate, department.EndDate, fmt.Sprint(department.DepartmentSize), department.FunctionalArea})
        }
    }
    departmentWriter.Flush()

    // Upload Departments to MinIO
    if err := uploadToMinIO(minioClient, bucketName, fmt.Sprintf("%s/departments/%s", databaseName, departmentFilename), departmentData.Bytes()); err != nil {
        log.Fatalf("Failed to upload department data to MinIO: %v", err)
    }

    fmt.Println("Data generation and upload complete.")
    totalTime := time.Since(startTime)
    fmt.Printf("Time taken for creating employees & departments: %s\n", totalTime-companyTime)
    fmt.Printf("Total time taken: %s\n", totalTime)
}
