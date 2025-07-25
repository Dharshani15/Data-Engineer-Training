PYTHON-PROGRAMMING LANGUAGE

print("Hello World)

name= "Hello, dharsh"
#slicing
print(name[0])
print(name[1:])
print(name[0:5])
#replace 
print(name.replace("dharsh","captain"))

#list
fruit=["apple","banana","orange"]
fruit.append("cherry")
fruit.remove("orange")
print(fruit[0])
print(fruit[0:])
for fruits in fruit:
    print(fruits)

#tuple
colors =("red","green","blue")
print(colors[1])
print(colors[1:3])

#TASK -1 
#1.Create a string variable movie = "The Lion King" Print only "Lion" using slicing.
movie="The Lion King"
print(movie[4:8])

#2.Make a list of your 4 favorite foods.
#Add one more.
#Remove the 2nd item.
#Print the final list.
foods=["dosa","idily","burger","pizza"]
foods.append("briyani")
foods.remove("idily")
for food in foods:
    print(food)

#3.Create a tuple of 3 numbers. Try accessing the last number.
numbers=(1,2,3)
print(numbers[2])

#if-else
age=int(input("Enter Your Age : "))
if age>=18:
    print("You can vote")
else:
    print("You can't vote")

#elif
marks=int(input("Enter your Marks : "))
if marks>=90:
    print("Grade A")
elif marks>=75:
    print("Grade B")
elif marks>=50:
    print("Grade C")
else :
    print("Grade F")

#while loop
count=1
while count<=5:
    print("Count is : ",count)
    count+=1

#for loop
for i in range(5):
    print(i)

for i in range(1,5):
    print(i)

for i in range (1,10):
    if i==5:
        continue
    if i==8:
        break
    print(i)

#function
#user defined
def greet():
    print("Hello Hexaware")
greet()

#with parameters
def greet(name):
    print(f"Hello,{name} ! Welcome")
greet("Dharsh")

def add(a,b):
    return a+b
result=add(10,5)
print(result)

def pow(base,exp=2):
    return base**exp
print(pow(5))
print(pow(5,3))

#in build function
n="name"
print(len(n))
print(type(n))

age="12"
print(age + "3")

name=["D","K","M","A"]
print(sorted(name))

print(abs(-9))
print(round(4.56745))
print(round(4.56733,3))

nums=[57,54,32,65,78]
print(max(nums))
print(min(nums))
print(sum(nums))

#modules
import math
print(math.sqrt(5))
print (math.pow(2,4))
print(math.pi)

import datetime
date1=datetime.date.today()
print("Todays date is :",date1)
time=datetime.datetime.now()
print("Current Time is:",time)












