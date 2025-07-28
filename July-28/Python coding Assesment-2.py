#Q1. Write a Python function factorial(n) using a loop.
def factorial(n):
    result = 1
    for i in range(1, n + 1):
        result *= i
    return result
num = int(input("Enter a number: "))
print("Factorial:", factorial(num))


#Q2. Create a list of tuples
students = [("Aarav", 80), ("Sanya", 65), ("Meera", 92), ("Rohan", 55)]
for name, score in students:
    if score > 75:
        print("Students scoring above 75:",name)
total_score = sum(score for _, score in students)
average_score = total_score / len(students)
print(f"\nAverage score: {average_score:.2f}")


#Q3. Create a class BankAccount 
class BankAccount:
    def __init__(self, holder_name, balance=0):
        self.holder_name = holder_name
        self.balance = balance

    def deposit(self, amount):
        self.balance += amount
        print(f"Deposited ₹{amount}. New balance: ₹{self.balance}")

    def withdraw(self, amount):
        if amount > self.balance:
            raise ValueError("Insufficient balance for withdrawal.")
        self.balance -= amount
        print(f"Withdrew ₹{amount}. New balance: ₹{self.balance}")

name = input("Enter account holder's name: ")
initial_balance = float(input("Enter initial balance: "))
account = BankAccount(name, initial_balance)
try:
    deposit_amount = float(input("Enter amount to deposit: "))
    account.deposit(deposit_amount)
    withdraw_amount = float(input("Enter amount to withdraw: "))
    account.withdraw(withdraw_amount)
except ValueError as e:
    print("Error:", e)


#Q4. Inherit a SavingsAccount class from BankAccount that adds:
class BankAccount:
    def __init__(self, holder_name, balance=0):
        self.holder_name = holder_name
        self.balance = balance

    def deposit(self, amount):
        self.balance += amount
        print(f"Deposited ₹{amount}. New balance: ₹{self.balance}")

    def withdraw(self, amount):
        if amount > self.balance:
            raise ValueError("Insufficient balance for withdrawal.")
        self.balance -= amount
        print(f"Withdrew ₹{amount}. New balance: ₹{self.balance}")

class SavingsAccount(BankAccount):
    def __init__(self, holder_name, balance=0, interest_rate=0.05):
        super().__init__(holder_name, balance)
        self.interest_rate = interest_rate

    def apply_interest(self):
        interest = self.balance * self.interest_rate
        self.balance += interest
        print(f"Interest of ₹{interest:.2f} applied. New balance: ₹{self.balance:.2f}")

try:
    name = input("Enter account holder's name: ")
    initial_balance = float(input("Enter initial balance: "))
    interest_rate = float(input("Enter interest rate (e.g., 0.05 for 5%): "))
    acc = SavingsAccount(name, initial_balance, interest_rate)
    acc.deposit(float(input("Enter amount to deposit: ")))
    acc.withdraw(float(input("Enter amount to withdraw: ")))
    acc.apply_interest()
except ValueError as e:
    print("Error:", e)


#Q5. Write a Python script using Pandas to •	Fill missing CustomerName with 'Unknown', Fill missing Quantity and Price with 0, Add column TotalAmount = Quantity * Price
 
import pandas as pd
df = pd.read_csv('orders.csv')
df['CustomerName'].fillna('Unknown', inplace=True)
df['Quantity'].fillna(0, inplace=True)
df['Price'].fillna(0, inplace=True)
df['TotalAmount'] = df['Quantity'] * df['Price']
df.to_csv('orders_cleaned.csv', index=False)
print("Data cleaned and saved to 'orders_cleaned.csv'.\n",df)


#Q6 JSON Task
import json
with open('inventory.json', 'r') as f:
    inventory = json.load(f)
for item in inventory:
    if item['stock'] > 0:
        item['status'] = 'In Stock'
    else:
        item['status'] = 'Out of Stock'
with open('inventory_updated.json', 'w') as f:
    json.dump(inventory, f, indent=4)
print("Updated inventory saved to 'inventory_updated.json'.")


#Q7 Enrichment with NumPy
import numpy as np
import pandas as pd
scores = np.random.randint(35, 101, size=20)
count_above_75 = np.sum(scores > 75)
mean_score = np.mean(scores)
std_deviation = np.std(scores)
df = pd.DataFrame({'Score': scores})
df.to_csv('scores.csv', index=False)
print("Student Scores:\n", scores)
print(f"\nNumber of students scoring above 75: {count_above_75}")
print(f"Mean score: {mean_score:.2f}")
print(f"Standard deviation: {std_deviation:.2f}")





























