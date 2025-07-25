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
















