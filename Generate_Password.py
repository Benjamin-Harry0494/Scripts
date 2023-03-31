import secrets
import string
import os
import time

def generatePassword(x):
    letters = string.ascii_letters
    digits = string.digits
    special_chars = string.punctuation

    alphabet = letters + digits + special_chars
    pwd = ''
    for i in range(int(x)):
        pwd += ''.join(secrets.choice(alphabet))

    return pwd
    


print("Please enter the length of your desired password:")
x =  input()
if x.isdigit() and int(x) <= 12:
    password = generatePassword(x)
    print("Your new password is: '" + password + "'")
    time.sleep(10)
    os.system('clear')
    exit()
else:
    print( x + " is not a valid input, please ensure that you are entering a whole number that is less than or equal to 12")
    exit()