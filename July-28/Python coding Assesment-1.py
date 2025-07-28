#Q1. Write a function is_prime(n) that returns True if n is a prime number, else False.
def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True
print(is_prime(7))
print(is_prime(10))  


#Q2. Write a program that:Accepts a string, Reverses it, Checks if it's a palindrome.
text = input("Enter a string: ")
reversed_text = text[::-1]
if text == reversed_text:
    print("It is a palindrome.")
else:
    print("It is not a palindrome.")


#Q3. Given a list of numbers, write code to:Remove duplicates, Sort them, Print the second largest number.
numbers = [5, 3, 8, 3, 1, 8, 9, 5]
unique_numbers = list(set(numbers))
unique_numbers.sort()
if len(unique_numbers) >= 2:
   print("Second largest number:", unique_numbers[-2])


#Q4. Create a base class Person with: Attributes: name , age, Method: display()
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def display(self):
        print(f"Name: {self.name}")
        print(f"Age: {self.age}")

class Employee(Person):
    def __init__(self, name, age, employee_id, department):
        super().__init__(name, age)
        self.employee_id = employee_id
        self.department = department

    def display(self):
        super().display()
        print(f"Employee ID: {self.employee_id}")
        print(f"Department: {self.department}")
emp = Employee("Aarav", 30, "EID01", "Finance")
emp.display()


#Q5. Demonstrate method overriding with another example: Vehicle → Car, drive() method with custom message in child
class Vehicle:
    def drive(self):
        print("The vehicle is being driven.")
class Car(Vehicle):
    def drive(self):
        print("The car is being driven smoothly on the highway.")

v = Vehicle()
v.drive()
c = Car()
c.drive()


#Q6. Read the students.csv and: Fill missing Age with average age, Fill missing Score with 0, Save the cleaned data as students_cleaned.csv
import pandas as pd
df = pd.read_csv('students.csv')
avg_age = df['Age'].mean()
df['Age'].fillna(avg_age, inplace=True)
df['Score'].fillna(0, inplace=True)
df.to_csv('students_cleaned.csv', index=False)
print("Missing values handled and cleaned data saved to 'students_cleaned.csv'.\n",df)


#Q7. Convert the cleaned CSV into JSON and save as stu
import pandas as pd
df = pd.read_csv('students_cleaned.csv')
df.to_json('students.json', orient='records', indent=4)
print("CSV data converted to JSON and saved as 'students.json'.")


#Q8. Using Pandas and NumPy, perform the following on students_csv
import pandas as pd
import numpy as np
df = pd.read_csv('students_cleaned.csv')
conditions = [
    (df['Score'] >= 85),
    (df['Score'] >= 60) & (df['Score'] < 85)
]
choices = ['Distinction', 'Passed']
df['Status'] = np.select(conditions, choices, default='Failed')
df['Tax_ID'] = 'TAX-' + df['ID'].astype(str)
df.to_csv('students_final.csv', index=False)
print(df)


#Q9. Write a script to: Read the JSON ,Increase all prices by 10%, Save back to products_updated.json
import json
with open('products.json', 'r') as f:
    products = json.load(f)
for product in products:
    product['price'] = round(product['price'] * 1.10, 2)
with open('products_updated.json', 'w') as f:
    json.dump(products, f, indent=4)
print("updated and  saved to 'products_updated.json'.")



