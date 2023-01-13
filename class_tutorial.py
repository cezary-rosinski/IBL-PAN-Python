#classes tutorial: https://www.youtube.com/watch?v=ZDa-Z5JzLYM

class Employee:
    pass

emp_1 = Employee()
emp_2 = Employee()

print(emp_1)
print(emp_2)

emp_1.first = 'Corey'
emp_1.last = 'Shafer'
emp_1.email = 'Corey.Schafer@company.com'
emp_1.pay = 50000

emp_2.first = 'Test'
emp_2.last = 'User'
emp_2.email = 'Test.User@company.com'
emp_2.pay = 60000

print(emp_1.email)
print(emp_2.email)

#%%

class Employee:
    
    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay
        self.email = first + '.' + last + '@company.com'
        
emp_1 = Employee('Corey', 'Schafer', 50000)
emp_2 = Employee('Test', 'User', 60000)

print(emp_1.email)
print(emp_2.email)

print('{} {}'.format(emp_1.first, emp_1.last))

#%%

class Employee:
    
    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay
        self.email = first + '.' + last + '@company.com'
        
    def fullname(self):
        return '{} {}'.format(self.first, self.last)

emp_1 = Employee('Corey', 'Schafer', 50000)
emp_2 = Employee('Test', 'User', 60000)
    
print(emp_1.fullname())
Employee.fullname(emp_1)

#%% https://www.youtube.com/watch?v=BJ-VvGyQxho instance variables vs. class variables

class Employee:
    
    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay
        self.email = first + '.' + last + '@company.com'
        
    def fullname(self):
        return '{} {}'.format(self.first, self.last)
    
    def apply_raise(self):
        self.pay = int(self.pay * 1.04)

emp_1 = Employee('Corey', 'Schafer', 50000)
emp_2 = Employee('Test', 'User', 60000)

print(emp_1.pay)
emp_1.apply_raise()
print(emp_1.pay)

emp_1.raise_amount
Employee.raise_amount

#%%

class Employee:
    
    raise_amount = 1.04
    
    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay
        self.email = first + '.' + last + '@company.com'
        
    def fullname(self):
        return '{} {}'.format(self.first, self.last)
    
    def apply_raise(self):
        self.pay = int(self.pay * self.raise_amount) #vs. Employee.raise_amount

emp_1 = Employee('Corey', 'Schafer', 50000)
emp_2 = Employee('Test', 'User', 60000)


print(emp_1.__dict__)
# print(Employee.__dict__)

emp_1.raise_amount = 1.05

print(Employee.raise_amount)
print(emp_1.raise_amount)
print(emp_2.raise_amount)

print(emp_1.__dict__)


#%% instance variables vs. class variables

class Employee:
    
    num_of_emps = 0
    raise_amount = 1.04
    
    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay
        self.email = first + '.' + last + '@company.com'
        
        Employee.num_of_emps += 1 # a nie self.num_of_emps, żeby pobierać z klasy, a nie z instancji
        
    def fullname(self):
        return '{} {}'.format(self.first, self.last)
    
    def apply_raise(self):
        self.pay = int(self.pay * self.raise_amount) #vs. Employee.raise_amount

print(Employee.num_of_emps)

emp_1 = Employee('Corey', 'Schafer', 50000)
emp_2 = Employee('Test', 'User', 60000)

print(Employee.num_of_emps)


#%% https://www.youtube.com/watch?v=rq8cL2XMM5M regular methods vs. classmethods vs. staticmethods

# regular uses self
# classmethods uses decorators

class Employee:
    
    num_of_emps = 0
    raise_amt = 1.04
    
    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay
        self.email = first + '.' + last + '@company.com'
        
        Employee.num_of_emps += 1 # a nie self.num_of_emps, żeby pobierać z klasy, a nie z instancji
        
    def fullname(self):
        return '{} {}'.format(self.first, self.last)
    
    def apply_raise(self):
        self.pay = int(self.pay * self.raise_amount) #vs. Employee.raise_amount
        
    @classmethod
    def set_raise_amt(cls, amount):
        cls.raise_amt = amount


emp_1 = Employee('Corey', 'Schafer', 50000)
emp_2 = Employee('Test', 'User', 60000)

# print(Employee.raise_amt)
# print(emp_1.raise_amt)
# print(emp_2.raise_amt)

# Employee.set_raise_amt(1.05) # to samo, co Employee.raise_amt = 1.05

# print(Employee.raise_amt)
# print(emp_1.raise_amt)
# print(emp_2.raise_amt)

emp_str_1 = 'John-Doe-70000'
emp_str_2 = 'Steve-Smith-30000'
emp_str_3 = 'Jane-Doe-90000'

first, last, pay = emp_str_1.split('-')

new_emp_1 = Employee(first, last, pay)

print(new_emp_1.email)
print(new_emp_1.pay)

#%% alternative constructor

class Employee:
    
    num_of_emps = 0
    raise_amt = 1.04
    
    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay
        self.email = first + '.' + last + '@company.com'
        
        Employee.num_of_emps += 1 # a nie self.num_of_emps, żeby pobierać z klasy, a nie z instancji
        
    def fullname(self):
        return '{} {}'.format(self.first, self.last)
    
    def apply_raise(self):
        self.pay = int(self.pay * self.raise_amount) #vs. Employee.raise_amount
        
    @classmethod
    def set_raise_amt(cls, amount):
        cls.raise_amt = amount
        
    @classmethod
    def from_string(cls, emp_str):
        first, last, pay = emp_str.split('-')
        return cls(first, last, pay)

emp_1 = Employee('Corey', 'Schafer', 50000)
emp_2 = Employee('Test', 'User', 60000)

emp_str_1 = 'John-Doe-70000'
emp_str_2 = 'Steve-Smith-30000'
emp_str_3 = 'Jane-Doe-90000'    

new_emp_1 = Employee.from_string(emp_str_1)

print(new_emp_1.email)
print(new_emp_1.pay)

#%% static methods
#regular methods = self
#classmethods = cls
#staticmethods = nie ma niczego automatycznego (ani self, ani cls)

class Employee:
    
    num_of_emps = 0
    raise_amt = 1.04
    
    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay
        self.email = first + '.' + last + '@company.com'
        
        Employee.num_of_emps += 1 # a nie self.num_of_emps, żeby pobierać z klasy, a nie z instancji
        
    def fullname(self):
        return '{} {}'.format(self.first, self.last)
    
    def apply_raise(self):
        self.pay = int(self.pay * self.raise_amount) #vs. Employee.raise_amount
        
    @classmethod
    def set_raise_amt(cls, amount):
        cls.raise_amt = amount
        
    @classmethod
    def from_string(cls, emp_str):
        first, last, pay = emp_str.split('-')
        return cls(first, last, pay)
    
    @staticmethod
    def is_workday(day):
        if day.weekday() == 5 or day.weekday() == 6:
            return False
        return True

emp_1 = Employee('Corey', 'Schafer', 50000)
emp_2 = Employee('Test', 'User', 60000)

import datetime
my_date = datetime.date(2016, 7, 11)

print(Employee.is_workday(my_date))

#%% https://www.youtube.com/watch?v=RSl87lqOXDE creating subclasses + inheritance

class Employee:
    
    raise_amt = 1.04
    
    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay
        self.email = first + '.' + last + '@company.com'
        
    def fullname(self):
        return '{} {}'.format(self.first, self.last)
    
    def apply_raise(self):
        self.pay = int(self.pay * self.raise_amt)

#method resolution order        
class Developer(Employee):
    pass

dev_1 = Developer('Corey', 'Schafer', 50000)
dev_2 = Employee('Test', 'User', 60000)

# print(dev_1.email)
# print(dev_2.email)

print(help(Developer))

#%%

class Developer(Employee):
    raise_amt = 1.10

dev_1 = Developer('Corey', 'Schafer', 50000)
dev_2 = Developer('Test', 'User', 60000)

print(dev_1.pay)
dev_1.apply_raise()
print(dev_1.pay)

#%% inheritance

class Developer(Employee):
    raise_amt = 1.10
    
    def __init__(self, first, last, pay, prog_lang):
        super().__init__(first, last, pay)
        self.prog_lang = prog_lang

dev_1 = Developer('Corey', 'Schafer', 50000, 'Python')
dev_2 = Developer('Test', 'User', 60000, 'Java')

print(dev_1.email)
print(dev_1.prog_lang)

#%%

class Manager(Employee):
    
    def __init__(self, first, last, pay, employees=None): #nigdy nie wpisywać domyślnie struktur danych w argumentach, dlatego tutaj nie daje pustej listy
        super().__init__(first, last, pay)
        if employees is None:
            self.employees = []
        else:
            self.employees = employees
            
    def add_emp(self, emp):
        if emp not in self.employees:
            self.employees.append(emp)
            
    def remove_emp(self, emp):
        if emp in self.employees:
            self.employees.remove(emp)
            
    def print_emps(self):
        for emp in self.employees:
            print('-->', emp.fullname())

mgr_1 = Manager('Sue', 'Smith', 90000, [dev_1])

print(mgr_1.email)

mgr_1.add_emp(dev_2)

mgr_1.print_emps()

mgr_1.remove_emp(dev_1)
mgr_1.print_emps()


#%%

print(isinstance(mgr_1, Manager))
print(isinstance(mgr_1, Employee))
print(isinstance(mgr_1, Developer))

print(issubclass(Developer, Employee))
print(issubclass(Manager, Employee))
print(issubclass(Manager, Developer))

#%% https://www.youtube.com/watch?v=3ohzBxoFHAY Python OOP Tutorial 5: Special (Magic/Dunder) Methods













