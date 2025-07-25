#1. FizzBuzz Challenge
for i in range(1,51):
    if i%3==0:
        print("Fizz")
    if i%5==0:
        print("Buzz")
    if i%3==0 and i%5==0:
        print("FizzBuzz")
    else :
        print(i)

#2. Login Simulation (Max 3 Attempts)
attempt=0
while attempt<3:
    username=input("enter username:")
    password=int(input("enter password:"))
    if username =="admin" and password==1234:
        print("Login succesfull")
        break
    else :
        print("Invalid.Try again")
        attempt+=1
if attempt==3:
    print("Account Hacked")

#3. Palindrome Checker
word=input()
if word == word[::-1]:
    print("Palindrome")
else:
    print("Not a palindrome")

#4.Prime Numbers in a Range
n = int(input("Enter a number: "))
for num in range(2, n + 1):
    for i in range(2, int(num ** 0.5) + 1):
        if num % i == 0:
            break
    else:
        print(num)

#5. Star Pyramid
n=int(input())
for i in range(n):
    for j in range(i):
        print("*",end="")
    print()

#6. Sum of Digits
n=int(input("Enter Number:"))
sum=0
while n>0:
    digit=n%10
    sum=sum+digit
    n=n//10
print(sum)

#7. Multiplication Table Generator
num=int(input("Enter Num:"))
for i in range(1,11):
    sum=i*num
    print(num,"x",i," =",sum)

#8. Count Vowels in a String
n=input("Enter input:")
count=0
for i in n:
    if i in "AEIOUaeiou":
        count+=1
print(count)
















