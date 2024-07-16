from faker import Faker
import csv
from tqdm import tqdm
import time


fake = Faker()

class SoftwareCompany:
    def __init__(self, name=None, industry=None, employees=None, revenue=None, location=None):
        self.name = name
        self.industry = industry
        self.employees = employees
        self.revenue = revenue
        self.location = location

    def get_fake_company(self, fake, unique_names):
        attempts = 0
        while True:
            attempts += 1
            self.name = fake.company()
            if self.name not in unique_names:
                break
            if attempts >= 100:
                self.name += f"-{fake.random_int(min=1, max=9999)}"
                break
        unique_names.add(self.name)
        
        self.industry = fake.bs()
        self.employees = fake.random_int(min=1, max=10000)
        self.revenue = fake.random_int(min=100000, max=1000000000)
        self.location = fake.city()
        return self

class Employee:
    def __init__(self, company_name):
        self.company_name = company_name
        self.first_name = None
        self.last_name = None
        self.email = None
        self.position = None
        self.salary = None

    def get_fake_employee(self, fake, company_domain):
        self.first_name = fake.first_name()
        self.last_name = fake.last_name()
        self.email = f"{self.first_name.lower()}.{self.last_name.lower()}@{company_domain}.com"
        self.position = fake.job()
        self.salary = fake.random_int(min=40000, max=1500000)
        return self
class Department:
    def __init__(self, company_name):
        self.company_name = company_name
        self.department_name = None
        self.manager_id = None  # Assume an employee ID will be the manager's ID
        self.budget = None
        self.location = None
        self.start_date = None
        self.end_date = None
        self.department_size = None
        self.functional_area = None

    def get_fake_department(self, fake):
        self.department_name = fake.job().split(" ")[0]  # Generating fake department names
        self.manager_id = fake.random_int(min=1, max=999999)
        self.budget = fake.random_int(min=10000, max=500000)
        self.location = fake.city()
        self.start_date = fake.date_this_decade()
        self.end_date = fake.future_date(end_date="+5y")
        self.department_size = fake.random_int(min=10, max=100)
        self.functional_area = fake.random_element(["R&D", "Sales", "Finance", "HR", "Operations"])
        return self

def create_and_write_departments(companies, department_csv_writer, fake):
    start_time = time.time()
    for company in tqdm(companies, desc="Creating departments"):
        num_departments = fake.random_int(min=1, max=3)
        for _ in tqdm(range(num_departments), desc=f"Departments for {company.name}", leave=False):
            new_department = Department(company.name).get_fake_department(fake)
            department_csv_writer.writerow([new_department.company_name, new_department.department_name,
                                            new_department.manager_id, new_department.budget, new_department.location,
                                            new_department.start_date, new_department.end_date, 
                                            new_department.department_size, new_department.functional_area])
    print(f"Time taken for creating departments: {time.time() - start_time} seconds")

    
def create_and_write_software_company(size, csv_writer, fake):
    start_time = time.time()
    companies = []
    unique_names = set()
    for _ in tqdm(range(size), desc="Creating companies"):
        new_company = SoftwareCompany().get_fake_company(fake, unique_names)
        companies.append(new_company)
        csv_writer.writerow([new_company.name, new_company.industry, new_company.employees, new_company.revenue, new_company.location])
    print(f"Time taken for creating companies: {time.time() - start_time} seconds")
    return companies

def create_and_write_employees(companies, employee_csv_writer, fake):
    start_time = time.time()
    for company in tqdm(companies, desc="Creating employees"):
        company_domain = company.name.replace(' ', '').replace('.', '').replace(',', '').lower()
        for _ in tqdm(range(company.employees), desc=f"Employees for {company.name}", leave=False):
            new_employee = Employee(company.name).get_fake_employee(fake, company_domain)
            employee_csv_writer.writerow([new_employee.company_name, new_employee.first_name, new_employee.last_name, new_employee.email, new_employee.position, new_employee.salary])
    print(f"Time taken for creating employees: {time.time() - start_time} seconds")

# Main script starts here
if __name__ == "__main__":
    # Create and write companies
    with open("companies.csv", "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["Name", "Industry", "Employees", "Revenue", "Location"])
        companies = create_and_write_software_company(int(input('Enter Amount of Companies to Create: ')), csv_writer, fake)
        csvfile.flush()

    # Create and write employees
    with open("employees.csv", "w", newline="") as csvfile:
        employee_csv_writer = csv.writer(csvfile)
        employee_csv_writer.writerow(["Company_Name", "First_Name", "Last_Name", "Email", "Position", "Salary"])
        create_and_write_employees(companies, employee_csv_writer, fake)
        csvfile.flush()

    # Create and write departments
    with open("departments.csv", "w", newline="") as csvfile:
        department_csv_writer = csv.writer(csvfile)
        department_csv_writer.writerow(["Company_Name", "Department_Name", "Manager_ID", "Budget", "Location",
                                        "Start_Date", "End_Date", "Department_Size", "Functional_Area"])
        create_and_write_departments(companies, department_csv_writer, fake)
        csvfile.flush()