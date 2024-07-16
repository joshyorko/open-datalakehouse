import csv
import io
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from faker import Faker
from tqdm import tqdm
import boto3
from src.creator import SoftwareCompany, Employee, Department
import uvicorn

fake = Faker()
app = FastAPI()

# Define Pydantic models
class CompanyRequest(BaseModel):
    size: int

class S3Details(BaseModel):
    bucket_name: str
    database_name: str

# API Endpoints
@app.post("/create_companies")
async def create_companies(request: CompanyRequest, s3_details: S3Details):
    try:
        s3_client = boto3.client('s3')
        companies = create_and_write_software_company_to_s3(request.size, s3_client, s3_details.bucket_name, s3_details.database_name, fake)
        return {"message": "Companies created successfully", "companies": companies}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def create_and_write_software_company_to_s3(size, s3_client, bucket_name, database_name, fake):
    start_time = time.time()
    companies = []
    unique_names = set()
    output = io.StringIO()
    csv_writer = csv.writer(output)

    # Write header
    csv_writer.writerow(["Name", "Industry", "Employees", "Revenue", "Location"])

    # Write data
    for _ in tqdm(range(size), desc="Creating companies"):
        new_company = SoftwareCompany().get_fake_company(fake, unique_names)
        companies.append(new_company)
        csv_writer.writerow([new_company.name, new_company.industry, new_company.employees, new_company.revenue, new_company.location])

    # Upload to S3
    output.seek(0)
    key = f"{database_name}/companies/data.csv"
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=output.getvalue())
    output.close()
    print(f"Time taken for creating companies: {time.time() - start_time} seconds")
    return companies

def create_and_write_employees_to_s3(companies, s3_client, bucket_name, database_name, fake):
    start_time = time.time()
    output = io.StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(["Company_Name", "First_Name", "Last_Name", "Email", "Position", "Salary"])

    for company in tqdm(companies, desc="Creating employees"):
        company_domain = company.name.replace(' ', '').replace('.', '').replace(',', '').lower()
        for _ in tqdm(range(company.employees), desc=f"Employees for {company.name}", leave=False):
            new_employee = Employee(company.name).get_fake_employee(fake, company_domain)
            csv_writer.writerow([new_employee.company_name, new_employee.first_name, new_employee.last_name, new_employee.email, new_employee.position, new_employee.salary])

    output.seek(0)
    key = f"{database_name}/employees/data.csv"
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=output.getvalue())
    output.close()
    print(f"Time taken for creating employees: {time.time() - start_time} seconds")

def create_and_write_departments_to_s3(companies, s3_client, bucket_name, database_name, fake):
    start_time = time.time()
    output = io.StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(["Company_Name", "Department_Name", "Manager_ID", "Budget", "Location", "Start_Date", "End_Date", "Department_Size", "Functional_Area"])

    for company in tqdm(companies, desc="Creating departments"):
        num_departments = fake.random_int(min=1, max=3)
        for _ in tqdm(range(num_departments), desc=f"Departments for {company.name}", leave=False):
            new_department = Department(company.name).get_fake_department(fake)
            csv_writer.writerow([new_department.company_name, new_department.department_name, new_department.manager_id, new_department.budget, new_department.location, new_department.start_date, new_department.end_date, new_department.department_size, new_department.functional_area])

    output.seek(0)
    key = f"{database_name}/departments/data.csv"
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=output.getvalue())
    output.close()
    print(f"Time taken for creating departments: {time.time() - start_time} seconds")

# Main script starts here
if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8000)
