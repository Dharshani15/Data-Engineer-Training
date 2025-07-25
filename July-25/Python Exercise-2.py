#1. BMI Calculator (Input + Function + Conditions + math )
import math
def calculate_bmi(weight, height):
    bmi = weight / math.pow(height, 2)
    return bmi
weight = float(input("Enter your weight in kg: "))
height = float(input("Enter your height in meters: "))
bmi = calculate_bmi(weight, height)
print(f"Your BMI is: {bmi:.2f}")
if bmi < 18.5:
    print("You are Underweight.")
elif 18.5 <= bmi < 25:
    print("You are Normal.")
else:
    print("You are Overweight.")


#2. Strong Password Checker (Strings + Conditions + Loop)
def is_strong(password):
    has_upper = any(c.isupper() for c in password)
    has_digit = any(c.isdigit() for c in password)
    has_special = any(c in "!@#$" for c in password)
    return has_upper and has_digit and has_special
while True:
    pwd = input("Enter a strong password: ")
    if is_strong(pwd):
        print("Password is strong!")
        break
    else:
        print("Weak password. Try again.")


#3. Weekly Expense Calculator (List + Loop + Built-in Functions)
def analyze_expenses(expenses):
    total = sum(expenses)
    average = total / len(expenses)
    highest = max(expenses)
    return total, average, highest
expenses = []
print("Enter your 7 daily expenses:")
for i in range(7):
    amt = float(input(f"Day {i+1} expense: "))
    expenses.append(amt)
total, avg, high = analyze_expenses(expenses)
print(f"Total spent: ₹{total}")
print(f"Average per day: ₹{avg:.2f}")
print(f"Highest spent in a day: ₹{high}")


#4. Guess the Number (Loops + random )
import random
secret = random.randint(1, 50)
chances = 5
for i in range(chances):
    guess = int(input("Guess the number (1 to 50): "))
    if guess == secret:
        print("Correct! You win ")
        break
    elif guess < secret:
        print("Too Low.")
    else:
        print("Too High.")
else:
    print(f"Out of chances! The number was {secret}.")


#5. Student Report Card (Functions + Input + If/Else + datetime )
import datetime
def report(marks):
    total = sum(marks)
    avg = total / len(marks)
    return total, avg
def get_grade(avg):
    if avg >= 75:
        return "A"
    elif avg >= 50:
        return "B"
    else:
        return "C"
name = input("Enter student name: ")
marks = []
for i in range(1, 4):
    m = float(input(f"Enter mark for subject {i}: "))
    marks.append(m)
total, avg = report(marks)
grade = get_grade(avg)
date = datetime.date.today()
print(f"Name: {name}")
print(f"Total Marks: {total}")
print(f"Average: {avg:.2f}")
print(f"Grade: {grade}")
print(f"Date: {date}")


#6. Contact Saver (Loop + Dictionary + File Writing)
contacts = {}
while True:
    print("\n1. Add Contact\n2. View Contacts\n3. Save & Exit")
    choice = input("Enter your choice: ")
    if choice == "1":
        name = input("Enter name: ")
        phone = input("Enter phone number: ")
        contacts[name] = phone
    elif choice == "2":
        if not contacts:
            print("No contacts yet.")
        else:
            for name, phone in contacts.items():
                print(f"{name} - {phone}")
    elif choice == "3":
        with open("contacts.txt", "w") as f:
            for name, phone in contacts.items():
                f.write(f"{name} - {phone}\n")
        print("Contacts saved. Goodbye!")
        break
    else:
        print("Invalid choice.")


