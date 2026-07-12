-- Create employee table
CREATE TABLE IF NOT EXISTS employee (
    emp_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    dept_id INT,
    salary NUMERIC(10,2),
    job_title VARCHAR(100),
    join_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create department table
CREATE TABLE IF NOT EXISTS department (
    dept_id SERIAL PRIMARY KEY,
    dept_name VARCHAR(100),
    cost_centre VARCHAR(50),
    manager_id INT,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create payroll table
CREATE TABLE IF NOT EXISTS payroll (
    payroll_id SERIAL PRIMARY KEY,
    emp_id INT,
    pay_period VARCHAR(20),
    gross_pay NUMERIC(10,2),
    net_pay NUMERIC(10,2),
    tax_code VARCHAR(20),
    ni_number VARCHAR(20),
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed departments
INSERT INTO department (dept_name, cost_centre) VALUES
('Engineering', 'CC001'),
('Finance', 'CC002'),
('HR', 'CC003'),
('Operations', 'CC004');

-- Seed employees
INSERT INTO employee (name, email, dept_id, salary, job_title, join_date) VALUES
('Alice Johnson', 'alice@minfy.com', 1, 50000, 'Software Engineer', '2023-01-15'),
('Bob Smith', 'bob@minfy.com', 2, 60000, 'Finance Analyst', '2022-06-01'),
('Carol White', 'carol@minfy.com', 3, 45000, 'HR Manager', '2021-09-10'),
('David Brown', 'david@minfy.com', 1, 55000, 'Senior Engineer', '2020-03-20'),
('Eve Davis', 'eve@minfy.com', 4, 48000, 'Operations Lead', '2023-07-01');

-- Seed payroll
INSERT INTO payroll (emp_id, pay_period, gross_pay, net_pay, tax_code, ni_number) VALUES
(1, '2026-06', 4166.67, 3200.00, '1257L', 'AB123456C'),
(2, '2026-06', 5000.00, 3800.00, '1257L', 'CD234567D'),
(3, '2026-06', 3750.00, 2900.00, '1257L', 'EF345678E'),
(4, '2026-06', 4583.33, 3500.00, '1257L', 'GH456789F'),
(5, '2026-06', 4000.00, 3100.00, '1257L', 'IJ567890G');